# Contributing to Raft Distributed Key Vault

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## Code of Conduct

- Be respectful and inclusive
- Focus on the code, not the person
- Help others learn and grow
- Report issues constructively

## Getting Started

### Prerequisites
- Java 17 or later
- Git
- Gradle (included via gradlew)
- Python 3.6+ (for testing tools)

### Setup Development Environment

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/raft-distributed-key-vault.git
   cd raft-distributed-key-vault
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Set up local environment**
   ```bash
   # Build the project
   ./gradlew clean build -x test
   
   # Run a single node
   NODE_ID=1 SERVER_PORT=8080 ./gradlew bootRun
   ```

4. **Run the test suite**
   ```bash
   ./tests/functional/run_test_suite.sh
   ```

## Making Changes

### Code Style

- **Java**: Follow Google Java Style Guide
  - 4-space indentation
  - Meaningful variable names
  - Add Javadoc for public methods
  
- **Shell Scripts**: Use shellcheck
  ```bash
  shellcheck tests/functional/*.sh
  ```

### Testing Requirements

Before submitting a PR, ensure:

1. **All tests pass**
   ```bash
   ./gradlew test
   ./tests/functional/run_test_suite.sh
   ```

2. **Add tests for new features**
   - Unit tests in `src/test/java/`
   - Integration tests if needed

3. **Verify no regressions**
   ```bash
   ./tests/validation/validate_phase_3_1.sh
   ```

### Commit Guidelines

- Write clear, descriptive commit messages
- Reference issues: "Fixes #123" or "Relates to #456"
- Keep commits atomic (one logical change per commit)

**Good commit message:**
```
Add leader election timeout optimization

- Implement exponential backoff for election timeouts
- Reduce average election time from 300ms to 150ms
- Add metrics tracking for election latency

Fixes #42
```

**Bad commit message:**
```
fixed stuff
```

## Submitting Pull Requests

### PR Checklist
- [ ] Code follows project style guidelines
- [ ] All tests pass locally
- [ ] Tests added/updated for new features
- [ ] Documentation updated (README, comments, etc.)
- [ ] No unnecessary dependencies added
- [ ] Commit messages are clear and descriptive

### PR Description Template
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Documentation update

## Testing
Describe how you tested these changes

## Related Issues
Fixes #123
```

### Review Process
1. Maintainer reviews code
2. Feedback/suggestions provided
3. You make requested changes
4. Approval and merge

## Areas for Contribution

### High Priority
- [ ] Phase 3.4: Multi-cluster federation
- [ ] Snapshotting performance optimization
- [ ] Production deployment guide
- [ ] Client library implementations

### Medium Priority
- [ ] Additional benchmarks
- [ ] Monitoring dashboard improvements
- [ ] Documentation enhancements
- [ ] Code examples and tutorials

### Low Priority
- [ ] Code formatting/cleanup
- [ ] Comments and documentation polish
- [ ] Additional test cases

## Reporting Bugs

1. **Check existing issues** to avoid duplicates
2. **Provide detailed description**:
   - What were you doing?
   - What did you expect?
   - What actually happened?
   - Relevant logs/errors

3. **Include environment info**:
   ```bash
   java -version
   ./gradlew --version
   uname -a
   ```

4. **Create reproducible example** if possible

## Documentation

### When to Update Docs
- Adding new features
- Changing API behavior
- Modifying test procedures
- Performance characteristics change

### Documentation Files
- [README.md](README.md) - Project overview
- [TESTING.md](TESTING.md) - Testing framework guide
- [CONTRIBUTING.md](CONTRIBUTING.md) - This file
- Inline code comments for complex logic

## Raft Implementation Guidelines

When modifying core Raft components:

1. **Understand the paper** - Review Raft consensus paper
2. **Maintain safety guarantees** - Don't compromise consistency
3. **Test failure scenarios** - Must handle edge cases
4. **Document changes** - Explain why, not just what
5. **Add metrics** - Track new behaviors

### Key Files (Don't modify without care)
```
src/main/java/com/omkar/distributed_key_vault/raft/
├── RaftState.java              # Core state - BE CAREFUL
├── ElectionService.java         # Leader election - TEST THOROUGHLY
└── controller/RaftRpcController.java  # RPC handling - VALIDATE
```

## Running Benchmarks

```bash
# Single run
./benchmarks/run-all.sh

# Parameter sweep
./benchmarks/run-sweep.sh

# Plot results
python3 benchmarks/plot.py --input benchmarks/results/latest/aggregate.csv
```

## Debugging

### Enable Debug Logging
1. Update `src/main/resources/application.yaml`:
   ```yaml
   logging:
     level:
       com.omkar.distributed_key_vault: DEBUG
   ```

2. Restart nodes

3. Check logs:
   ```bash
   # From Spring Boot console or log files
   ```

### Running with Debugger
```bash
# IntelliJ IDEA
right-click DistributedKeyVaultApplication.java → Debug

# Command line with remote debugging
./gradlew bootRun --debug-jvm
# Then attach debugger to localhost:5005
```

## Performance Considerations

When contributing:
- Avoid blocking operations in election/replication paths
- Use async operations where possible
- Profile changes with benchmarks
- Consider memory impact of new features

## Questions?

- Check existing issues and discussions
- Review [TESTING.md](TESTING.md) for framework details
- Look at existing code for patterns
- Ask in pull request discussions

## Recognition

Contributors are recognized in:
- Project README
- GitHub contributors page
- Release notes

Thank you for contributing! 🎉
