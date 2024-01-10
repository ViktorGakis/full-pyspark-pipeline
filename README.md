## Containarization

We have containerized our development environment along with an mysql database server and phpadmin.

In order to create this dev environment please run

```bash
docker-compose up -d
```

Note that we have create a quite involved docker image `Dockerfile.pythondev` that includes all the needed groundwork for spark/pysparκ in order to work.