# Contributing

## Code Style
Install pre-commit hooks to ensure code style is consistent before committing.
```shell
pre-commit install
```

Or run the style checkers manually.
```shell
sbt scalafmt
sbt scalastyle
```

## Publishing
When you push a new *.*.* (for example 0.1.0) tag to the repository, the GitHub Action will automatically package and
create a new release.