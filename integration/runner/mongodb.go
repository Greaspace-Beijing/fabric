/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package runner

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/pkg/errors"
	"github.com/tedsuo/ifrit"
)

const MongoDBDefaultImage = "mongo:latest"

// MongoDB manages the execution of an instance of a dockerized MongoDB
// for tests.
type MongoDB struct {
	Client        *docker.Client
	Image         string
	HostIP        string
	HostPort      int
	ContainerPort docker.Port
	Name          string
	StartTimeout  time.Duration

	ErrorStream  io.Writer
	OutputStream io.Writer

	containerID      string
	hostAddress      string
	containerAddress string
	address          string

	mutex   sync.Mutex
	stopped bool
}

// Run runs a MongoDB container. It implements the ifrit.Runner interface
func (m *MongoDB) Run(sigCh <-chan os.Signal, ready chan<- struct{}) error {
	fmt.Println("RUN")
	if m.Image == "" {
		m.Image = MongoDBDefaultImage
	}

	if m.Name == "" {
		m.Name = DefaultNamer()
	}

	if m.HostIP == "" {
		m.HostIP = "127.0.0.1"
	}

	if m.ContainerPort == docker.Port("") {
		m.ContainerPort = docker.Port("27017/tcp")
	}

	if m.StartTimeout == 0 {
		m.StartTimeout = DefaultStartTimeout
	}

	if m.Client == nil {
		client, err := docker.NewClientFromEnv()
		if err != nil {
			return err
		}
		m.Client = client
	}

	hostConfig := &docker.HostConfig{
		AutoRemove: true,
		PortBindings: map[docker.Port][]docker.PortBinding{
			m.ContainerPort: {{
				HostIP:   m.HostIP,
				HostPort: strconv.Itoa(m.HostPort),
			}},
		},
	}

	container, err := m.Client.CreateContainer(
		docker.CreateContainerOptions{
			Name:       m.Name,
			Config:     &docker.Config{Image: m.Image},
			HostConfig: hostConfig,
		},
	)
	if err != nil {
		return err
	}
	m.containerID = container.ID

	err = m.Client.StartContainer(container.ID, nil)
	if err != nil {
		return err
	}
	defer m.Stop()

	container, err = m.Client.InspectContainer(container.ID)
	if err != nil {
		return err
	}
	m.hostAddress = net.JoinHostPort(
		container.NetworkSettings.Ports[m.ContainerPort][0].HostIP,
		container.NetworkSettings.Ports[m.ContainerPort][0].HostPort,
	)
	m.containerAddress = net.JoinHostPort(
		container.NetworkSettings.IPAddress,
		m.ContainerPort.Port(),
	)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	go m.streamLogs(streamCtx)

	containerExit := m.wait()
	ctx, cancel := context.WithTimeout(context.Background(), m.StartTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), "database in container %s did not start", m.containerID)
	case <-containerExit:
		return errors.New("container exited before ready")
	case <-m.ready(ctx, m.hostAddress):
		m.address = m.hostAddress
	case <-m.ready(ctx, m.containerAddress):
		m.address = m.containerAddress
	}

	cancel()
	close(ready)

	for {
		select {
		case err := <-containerExit:
			return err
		case <-sigCh:
			if err := m.Stop(); err != nil {
				return err
			}
		}
	}
}

//func endpointReady(ctx context.Context, url string) bool {
//	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
//	defer cancel()
//
//	req, err := http.NewRequest(http.MethodGet, url, nil)
//	if err != nil {
//		return false
//	}
//
//	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
//	return err == nil && resp.StatusCode == http.StatusOK
//}

func (m *MongoDB) ready(ctx context.Context, addr string) <-chan struct{} {
	readyCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err == nil {
				conn.Close()
				close(readyCh)
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	return readyCh
}

func (m *MongoDB) wait() <-chan error {
	exitCh := make(chan error)
	go func() {
		exitCode, err := m.Client.WaitContainer(m.containerID)
		if err == nil {
			err = fmt.Errorf("mongodb: process exited with %d", exitCode)
		}
		exitCh <- err
	}()

	return exitCh
}

func (m *MongoDB) streamLogs(ctx context.Context) {
	if m.ErrorStream == nil && m.OutputStream == nil {
		return
	}

	logOptions := docker.LogsOptions{
		Context:      ctx,
		Container:    m.containerID,
		Follow:       true,
		ErrorStream:  m.ErrorStream,
		OutputStream: m.OutputStream,
		Stderr:       m.ErrorStream != nil,
		Stdout:       m.OutputStream != nil,
	}

	err := m.Client.Logs(logOptions)
	if err != nil {
		fmt.Fprintf(m.ErrorStream, "log stream ended with error: %s", err)
	}
}

// Address returns the address successfully used by the readiness check.
func (m *MongoDB) Address() string {
	return m.address
}

// HostAddress returns the host address where this MongoDB instance is available.
func (m *MongoDB) HostAddress() string {
	return m.hostAddress
}

// ContainerAddress returns the container address where this MongoDB instance
// is available.
func (m *MongoDB) ContainerAddress() string {
	return m.containerAddress
}

// ContainerID returns the container ID of this MongoDB
func (m *MongoDB) ContainerID() string {
	return m.containerID
}

// Start starts the MongoDB container using an ifrit runner
func (m *MongoDB) Start() error {
	p := ifrit.Invoke(m)

	select {
	case <-p.Ready():
		return nil
	case err := <-p.Wait():
		return err
	}
}

// Stop stops and removes the MongoDB container
func (m *MongoDB) Stop() error {
	fmt.Println("Stop")
	m.mutex.Lock()
	if m.stopped {
		m.mutex.Unlock()
		return errors.Errorf("container %s already stopped", m.containerID)
	}
	m.stopped = true
	m.mutex.Unlock()

	err := m.Client.StopContainer(m.containerID, 0)
	if err != nil {
		return err
	}

	return nil
}
