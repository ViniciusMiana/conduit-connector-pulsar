# Conduit Connector for <resource>
[Conduit](https://conduit.io) for <resource>.

## How to build?
Run `make build` to build the connector.

## Testing
Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

## Source
A source connector pulls data from an external resource and pushes it to downstream resources via Conduit.

### Configuration

| name                 | description                                                | required | default value             |
|----------------------|------------------------------------------------------------|----------|---------------------------|
| `URL`                | The pulsar URL                                             | true     |                           |
| `topic`              | Topic is the Pulsar topic from which records will be read. | true     |                           |

## Destination
A destination connector pushes data from upstream resources to an external resource via Conduit.

### Configuration

| name                 | description                                                   | required | default value             |
|----------------------|---------------------------------------------------------------|----------|---------------------------|
| `URL`                | The pulsar URL                                                | true     |                           |
| `topic`              | Topic is the Pulsar topic from which records will be written. | true     |                           |

## Known Issues & Limitations
* Known issue A
* Limitation A

## Planned work
- [ ] Item A
- [ ] Item B
