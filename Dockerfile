FROM cosmtrek/air

RUN apt-get update && \
	apt-get install -y --no-install-recommends poppler-utils && \
	rm -rf /var/lib/apt/lists/*

