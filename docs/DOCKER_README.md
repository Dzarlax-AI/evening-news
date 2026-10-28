# Docker runtime

The core build graph produces separate web, background and domain images from `docker/Dockerfile`. The background image runs the worker, scheduler, ingestion and processing components in one process; the domain image runs the private domain process. The extractor and shared browser remain separate services. The domain image uses the explicit `news-migrate` command for schema maintenance; startup does not apply DDL.

For local configuration and commands, see [Quick start](QUICKSTART.md). For the runtime boundary and current acceptance status, see [Private Go domain runtime](go-domain-runtime.md). Image build and publication requirements are in [Core image build and release](core-image-build-and-release.md).
