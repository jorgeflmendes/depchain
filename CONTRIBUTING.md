# Contributing to DepChain

Thanks for your interest in contributing.

## Development Setup

1. Install Java 21+ and Maven 3.6.3+.
2. Clone the repository.
3. Build once to validate your environment:

```powershell
mvn clean verify
```

## Branch and Commit Guidelines

1. Create a feature branch from `main`.
2. Keep commits focused and descriptive.
3. Reference issue numbers in commit messages and pull requests when applicable.

## Pull Request Checklist

1. Build succeeds locally with:

```powershell
mvn clean verify
```

2. New behavior includes tests when feasible.
3. Documentation is updated for user-facing changes.
4. No merge conflict markers or TODO/FIXME placeholders are introduced.

## Coding Style

- Follow existing project conventions and package structure.
- Keep methods cohesive and avoid unrelated refactors in the same PR.
- Prefer explicit validation and clear error messages.

## Reporting Bugs

Open a GitHub issue and include:

1. Expected behavior.
2. Actual behavior.
3. Steps to reproduce.
4. Environment details (OS, Java, Maven).

## Security Issues

Do not open public issues for vulnerabilities. Follow [SECURITY.md](SECURITY.md).
