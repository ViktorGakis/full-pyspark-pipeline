## Containarization

We have containerized our development environment along with an mysql database server and phpadmin.

In order to create this dev environment please run

```bash
docker-compose up -d
```

Note that we have created a quite involved docker image `Dockerfile.pythondev` that includes all the needed groundwork for spark/pysparκ to be properly installed.