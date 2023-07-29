FROM python:3.11-slim AS build

RUN pip3 install poetry
WORKDIR /src
ADD . /src
RUN poetry install
RUN poetry build -f wheel


FROM python:3.11-slim

WORKDIR /pkg
COPY --from=build /src/dist/prometheus_block_exporter*.whl /pkg/
RUN pip3 install prometheus_block_exporter*.whl
ENTRYPOINT ["python3", "-u", "-m", "prometheus_block_exporter"]
