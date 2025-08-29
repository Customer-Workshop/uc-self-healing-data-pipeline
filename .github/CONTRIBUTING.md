# Contributing to Autonomous Data Pipeline Orchestrator

Thank you for your interest in contributing to the Autonomous Data Pipeline Orchestrator! This project aims to revolutionize data engineering operations through intelligent automation and self-healing capabilities.

## 🎯 Project Goals

- **Autonomous Operations**: Reduce manual intervention in data pipeline operations
- **Intelligent Decision Making**: Use AI/ML to make smart remediation decisions
- **Production Ready**: Build enterprise-grade, scalable data infrastructure
- **Developer Experience**: Provide excellent tooling and documentation

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Git
- 8GB+ RAM recommended

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/Autonomous-Orchestrator-Ai.git
   cd Autonomous-Orchestrator-Ai
   ```

2. **Development Environment**
   ```bash
   make dev-setup
   ```

3. **Start Services**
   ```bash
   make up
   ```

4. **Run Tests**
   ```bash
   make test
   ```

## 🔄 Development Workflow

### 1. Choose an Issue
- Look for issues labeled `good first issue` for beginners
- Check `help wanted` for areas where contributions are needed
- Comment on the issue to indicate you're working on it

### 2. Create a Branch
```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

### 3. Development Process
```bash
# Make your changes
vim agent/decision_agent.py

# Run tests frequently
make test

# Check code quality
make fmt lint

# Test integration
make up
make airflow-trigger
```

### 4. Commit Guidelines
We follow [Conventional Commits](https://www.conventionalcommits.org/):

```bash
# Format: type(scope): description
git commit -m "feat(agent): add schema drift auto-remediation"
git commit -m "fix(kafka): resolve connection timeout issue"
git commit -m "docs(readme): update quick start guide"
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `perf`: Performance improvements

### 5. Pull Request Process
1. Push your branch to your fork
2. Create a Pull Request using our template
3. Ensure all CI checks pass
4. Address review feedback
5. Maintain a clean commit history

## 🧪 Testing Guidelines

### Test Types
- **Unit Tests**: Test individual functions and classes
- **Integration Tests**: Test component interactions
- **End-to-End Tests**: Test complete workflows

### Running Tests
```bash
# All tests
make test

# Specific test suites
make test-agent
make test-dq
make test-spark

# With coverage
make test-coverage
```

### Writing Tests
- Place tests in the `tests/` directory
- Follow the naming convention: `test_*.py`
- Use descriptive test names
- Include docstrings explaining test purpose
- Mock external dependencies

Example:
```python
def test_agent_handles_schema_drift():
    """Test that agent correctly detects and handles schema drift."""
    # Given
    context = create_test_context(schema_drift=True)
    agent = PipelineDecisionAgent()
    
    # When
    decision = agent.make_decision(context)
    
    # Then
    assert decision.decision == "apply_schema_remap"
    assert decision.confidence > 0.8
```

## 📋 Code Standards

### Python Style Guide
- Follow [PEP 8](https://pep8.org/)
- Use [Black](https://black.readthedocs.io/) for formatting
- Use [isort](https://pycqa.github.io/isort/) for import sorting
- Use [Ruff](https://github.com/astral-sh/ruff) for linting

### Code Quality Tools
```bash
# Format code
make fmt

# Lint code
make lint

# Security scan
bandit -r agent ops kafka/producer
```

### Documentation Standards
- **Docstrings**: All public functions and classes must have docstrings
- **Type Hints**: Use type hints for function parameters and return values
- **Comments**: Explain complex logic and business rules
- **README Updates**: Update documentation for new features

### Architecture Principles
- **Modularity**: Keep components loosely coupled
- **Testability**: Design for easy testing
- **Observability**: Include logging and metrics
- **Error Handling**: Graceful error handling and recovery
- **Configuration**: Use environment variables and config files

## 🏗️ Component Architecture

### Core Components
1. **Decision Agent** (`agent/`): AI-powered decision making
2. **Data Quality** (`dq/`): Great Expectations integration
3. **Operations** (`ops/`): Notifications and incident management
4. **Kafka Layer** (`kafka/`): Event streaming and schemas
5. **Spark Jobs** (`spark/`): Stream processing
6. **Airflow DAGs** (`airflow/`): Orchestration

### Adding New Features

#### New Decision Logic
1. Add logic to `agent/decision_agent.py`
2. Add corresponding tools in `agent/tools.py`
3. Update tests in `tests/test_agent.py`
4. Document the decision matrix in README

#### New Data Sources
1. Extend Kafka schemas in `kafka/schemas/`
2. Update Spark job in `spark/jobs/`
3. Add data quality expectations in `dq/`
4. Update Airflow DAG if needed

#### New Remediation Actions
1. Add tool to `agent/tools.py`
2. Update agent decision logic
3. Add tests for the new tool
4. Document the new capability

## 🐛 Bug Reports

When reporting bugs:
1. Use the bug report template
2. Include reproduction steps
3. Provide environment details
4. Include relevant logs
5. Add screenshots if applicable

## 💡 Feature Requests

When suggesting features:
1. Use the feature request template
2. Explain the use case and problem
3. Propose a solution
4. Consider implementation complexity
5. Discuss backward compatibility

## 🔒 Security

### Security Guidelines
- Never commit secrets or credentials
- Use environment variables for sensitive data
- Follow security best practices for data handling
- Report security vulnerabilities privately

### Reporting Security Issues
Email security issues to: abdulsohail018@gmail.com

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## 📊 Performance Considerations

### Performance Guidelines
- Profile code for performance bottlenecks
- Consider memory usage for large datasets
- Optimize database queries
- Use caching where appropriate
- Monitor resource usage

### Benchmarking
```bash
# Run performance tests
make performance-test

# Profile memory usage
python -m memory_profiler script.py
```

## 🤝 Community

### Communication Channels
- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: Questions and ideas
- **Pull Requests**: Code contributions

### Code of Conduct
- Be respectful and inclusive
- Welcome newcomers and help them learn
- Focus on constructive feedback
- Assume good intentions

## 📋 Review Process

### For Contributors
- Respond to feedback promptly
- Keep PRs focused and small
- Write clear commit messages
- Update documentation

### For Reviewers
- Provide constructive feedback
- Focus on code quality and maintainability
- Check test coverage
- Verify documentation updates

## 🎉 Recognition

Contributors will be recognized in:
- README.md contributors section
- Release notes
- GitHub contributors graph

Significant contributors may be invited to become maintainers.

## 📚 Resources

### Learning Resources
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [LangChain Documentation](https://python.langchain.com/)

### Development Tools
- [Docker Documentation](https://docs.docker.com/)
- [Python Best Practices](https://realpython.com/python-code-quality/)
- [Git Workflow](https://guides.github.com/introduction/git-handbook/)

## ❓ Questions?

- Check existing [GitHub Issues](https://github.com/AbdulSohail018/Autonomous-Orchestrator-Ai/issues)
- Start a [GitHub Discussion](https://github.com/AbdulSohail018/Autonomous-Orchestrator-Ai/discussions)
- Review the [documentation](README.md)

Thank you for contributing to the future of autonomous data engineering! 🚀