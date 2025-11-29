package main

import (
	"os"
	"os/exec"
)

type DesktopStreamer struct {
	cmd  *exec.Cmd
	src  *LiveStreamSource
	done chan error
}

func NewDesktopStreamer(src *LiveStreamSource) *DesktopStreamer {
	// Put your exact ffmpeg args here
	args := []string{
		"-f", "avfoundation",
		"-pixel_format", "uyvy422",
		"-i", "1:none",
		"-video_size", "800x400",
		"-r", "30",
		"-vf", "scale=800:400:force_original_aspect_ratio=decrease,pad=800:400:(ow-iw)/2:(oh-ih)/2,setsar=1",
		"-c:v", "libx264", "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
		"-preset", "veryfast", "-tune", "zerolatency",
		"-x264-params", "cabac=0:bframes=0:weightp=0:scenecut=0:keyint=1:min-keyint=1:slices=6:slice-max-size=1200:vbv-maxrate=2000:repeat-headers=1",
		"-b:v", "2000k", "-maxrate", "2000k", "-bufsize", "2000k",
		"-bsf:v", "h264_metadata=aud=insert",
		"-f", "h264", "-",
	}

	return &DesktopStreamer{
		cmd:  exec.Command("ffmpeg", args...),
		src:  src,
		done: make(chan error, 1),
	}
}

func (d *DesktopStreamer) Start() error {
	stdout, err := d.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	d.cmd.Stderr = os.Stderr

	if err = d.cmd.Start(); err != nil {
		return err
	}

	go func() {
		err = feedStreamToLiveSource(stdout, d.src)
		// Wait for ffmpeg to exit too, so we don't leak
		_ = d.cmd.Wait()
		d.done <- err
	}()

	return nil
}

func (d *DesktopStreamer) Stop() error {
	return <-d.done
}
