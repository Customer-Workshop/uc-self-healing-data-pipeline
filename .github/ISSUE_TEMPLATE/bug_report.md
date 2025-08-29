---
name: Bug Report
about: Create a report to help us improve the pipeline
title: '[BUG] '
labels: 'bug'
assignees: ''

---

## 🐛 Bug Description
A clear and concise description of what the bug is.

## 🔄 Steps to Reproduce
Steps to reproduce the behavior:
1. Go to '...'
2. Click on '....'
3. Scroll down to '....'
4. See error

## ✅ Expected Behavior
A clear and concise description of what you expected to happen.

## 📸 Screenshots
If applicable, add screenshots to help explain your problem.

## 🛠️ Environment
- OS: [e.g. Ubuntu 20.04]
- Docker Version: [e.g. 20.10.7]
- Docker Compose Version: [e.g. 1.29.2]
- Python Version: [e.g. 3.10]
- Pipeline Version: [e.g. v1.0.0]

## 📋 Pipeline Configuration
```yaml
# Paste relevant configuration from .env or config files
```

## 📄 Logs
```
# Paste relevant log outputs
# Use `make logs` or `docker-compose logs <service>`
```

## 🔍 Additional Context
Add any other context about the problem here.

## 🏥 Health Check
Please run the following commands and paste the output:

```bash
make status
make debug
```

## 📊 Data Context
- Approximate data volume: [e.g. 1000 msgs/sec]
- Schema version: [e.g. v1, v2 with drift]
- Late arrival percentage: [e.g. 5%]
- DQ failure rate: [e.g. 2%]