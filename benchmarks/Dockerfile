FROM python:3.11-slim

WORKDIR /bench

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# copy runner scripts and requirements (source paths are relative to repo root build context)
COPY benchmarks/run-sweep-container.sh /bench/run-sweep-container.sh
COPY benchmarks/resource_sampler.sh /bench/resource_sampler.sh
COPY benchmarks/requirements.txt /bench/requirements.txt
RUN chmod +x /bench/run-sweep-container.sh /bench/resource_sampler.sh

RUN python -m pip install --no-cache-dir -r /bench/requirements.txt

ENTRYPOINT ["/bench/run-sweep-container.sh"]
