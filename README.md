# Hyblock Data Fetcher


# Client/API Keys as Environment Variables
Before executing the `main.py`, the user should either
1. Hardcoded the client_id/client_secret/api_key in `main.py`
2. export client_id/client_secret/api_key as environment variables.

In Lunix/MacOS, users can type following command inside terminalto set the environment variables

```bash
export CLIENT_ID=YOUR_CLIENT_ID && export CLINET_SECRET=YOUR_CLIENT_SECRET && export API_KEY=YOUR_API_KEY
```


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
