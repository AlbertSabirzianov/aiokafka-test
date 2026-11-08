# Contributing to aiokafka-test

Thank you for your interest in **aiokafka-test**! We welcome contributions from everyone — whether it's bug fixes, new features, documentation improvements, or simply reporting issues.

## Getting Started

### Prerequisites

- Python >= 3.11
- [uv](https://github.com/astral-sh/uv) — the fast Python package installer and resolver

### Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/your-username/aiokafka-test.git
cd aiokafka-test
uv sync
```

This will create a virtual environment and install all required dependencies.

## Running Tests

All tests are written using Python's built-in `unittest` framework. To run the full test suite:

```bash
python -m unittest
```

To run a specific test file:

```bash
python -m unittest tests.test_aiokafka_test_context
```

To run a single test method:

```bash
python -m unittest tests.test_aiokafka_test_context.TestAiokafkaTestContext.test_send_message_with_value
```

## Development Roadmap

Here are some features we plan to implement in the future:

- **Message assertions** — helper methods for common test assertions (e.g., `assert_topic_has_messages`, `assert_message_value`)
- **CI integration** — automated testing on pull requests
- **Type hints** — full type coverage across the library

If you have ideas for additional features, feel free to open an issue or submit a pull request!

## How to Contribute

### Reporting Issues

If you find a bug or have a feature request, please [open an issue](https://github.com/your-username/aiokafka-test/issues) with:

- A clear description of the problem
- Steps to reproduce (for bugs)
- Your Python and aiokafka versions
- Any relevant error messages or stack traces

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run the tests to ensure everything passes: `python -m unittest`
5. Commit your changes: `git commit -m "Add my feature"`
6. Push to the branch: `git push origin feature/my-feature`
7. Open a Pull Request

### Code Style

- Follow PEP 8 conventions
- Use meaningful variable and function names
- Add docstrings to new functions and classes
- Keep tests focused and descriptive

## Questions?

If you have any questions or need help getting started, feel free to open an issue and ask. We're happy to help!

## License

By contributing to aiokafka-test, you agree that your contributions will be licensed under the project's MIT License.
