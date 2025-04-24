# Hyblock Data Fetcher

# Set up (uv, Python version = ^3.12)
Use `uv` to set up a new python virtual environment and then run the codes
- `uv sync`
- `python -m venv .venv`
- `source .venv/bin/activate`
- `uv run main.py`



# Set up (pip, Python version = ^3.12)
Use pip to install required dependencies listed inside `pyproject.toml`
- `python -m venv .venv`
- `source .venv/bin/activate`
- `make init-dev` (only capable in MacOS) / `pip install -r dev.txt -r requirements.txt` (all platforms)
- `python main.py `
