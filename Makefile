build:
	docker buildx build --progress=plain --no-cache \
		-t streamnative:latest .
