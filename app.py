==> Cloning from https://github.com/Rudy1128/btc-strike-ai
==> Checking out commit 44329241216c8f833536ccf8dd3f3aa194d21392 in branch main
==> Using Python version 3.14.3 (default)
==> Docs on specifying a Python version: https://render.com/docs/python-version
==> Installing Python version 3.14.3...
==> Using Poetry version 2.1.3 (default)
==> Docs on specifying a Poetry version: https://render.com/docs/poetry-version
==> Running build command 'pip install -r requirements.txt'...
Collecting Flask (from -r requirements.txt (line 1))
  Downloading flask-3.1.3-py3-none-any.whl.metadata (3.2 kB)
Collecting requests (from -r requirements.txt (line 2))
  Downloading requests-2.34.2-py3-none-any.whl.metadata (4.8 kB)
Collecting websocket-client==1.8.0 (from -r requirements.txt (line 3))
  Downloading websocket_client-1.8.0-py3-none-any.whl.metadata (8.0 kB)
Collecting gunicorn (from -r requirements.txt (line 4))
  Downloading gunicorn-26.2.0-py3-none-any.whl.metadata (5.5 kB)
Collecting blinker>=1.9.0 (from Flask->-r requirements.txt (line 1))
  Downloading blinker-1.9.0-py3-none-any.whl.metadata (1.6 kB)
Collecting click>=8.1.3 (from Flask->-r requirements.txt (line 1))
  Downloading click-8.5.0-py3-none-any.whl.metadata (2.6 kB)
Collecting itsdangerous>=2.2.0 (from Flask->-r requirements.txt (line 1))
  Downloading itsdangerous-2.2.0-py3-none-any.whl.metadata (1.9 kB)
Collecting jinja2>=3.1.2 (from Flask->-r requirements.txt (line 1))
  Downloading jinja2-3.1.6-py3-none-any.whl.metadata (2.9 kB)
Collecting markupsafe>=2.1.1 (from Flask->-r requirements.txt (line 1))
  Downloading markupsafe-3.0.4-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl.metadata (2.7 kB)
Collecting werkzeug>=3.1.0 (from Flask->-r requirements.txt (line 1))
  Downloading werkzeug-3.1.9-py3-none-any.whl.metadata (4.1 kB)
Collecting charset_normalizer<4,>=2 (from requests->-r requirements.txt (line 2))
  Downloading charset_normalizer-3.5.2-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl.metadata (46 kB)
Collecting idna<4,>=2.5 (from requests->-r requirements.txt (line 2))
  Downloading idna-3.20-py3-none-any.whl.metadata (7.2 kB)
Collecting urllib3<3,>=1.26 (from requests->-r requirements.txt (line 2))
  Downloading urllib3-2.8.0-py3-none-any.whl.metadata (7.4 kB)
Collecting certifi>=2023.5.7 (from requests->-r requirements.txt (line 2))
  Downloading certifi-2026.7.22-py3-none-any.whl.metadata (2.5 kB)
Downloading websocket_client-1.8.0-py3-none-any.whl (58 kB)
Downloading flask-3.1.3-py3-none-any.whl (103 kB)
Downloading requests-2.34.2-py3-none-any.whl (73 kB)
Downloading charset_normalizer-3.5.2-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl (255 kB)
Downloading idna-3.20-py3-none-any.whl (69 kB)
Downloading urllib3-2.8.0-py3-none-any.whl (135 kB)
Downloading gunicorn-26.2.0-py3-none-any.whl (228 kB)
Downloading blinker-1.9.0-py3-none-any.whl (8.5 kB)
Downloading certifi-2026.7.22-py3-none-any.whl (136 kB)
Downloading click-8.5.0-py3-none-any.whl (125 kB)
Downloading itsdangerous-2.2.0-py3-none-any.whl (16 kB)
Downloading jinja2-3.1.6-py3-none-any.whl (134 kB)
Downloading markupsafe-3.0.4-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl (23 kB)
Downloading werkzeug-3.1.9-py3-none-any.whl (228 kB)
Installing collected packages: websocket-client, urllib3, markupsafe, itsdangerous, idna, gunicorn, click, charset_normalizer, certifi, blinker, werkzeug, requests, jinja2, Flask

Successfully installed Flask-3.1.3 blinker-1.9.0 certifi-2026.7.22 charset_normalizer-3.5.2 click-8.5.0 gunicorn-26.2.0 idna-3.20 itsdangerous-2.2.0 jinja2-3.1.6 markupsafe-3.0.4 requests-2.34.2 urllib3-2.8.0 websocket-client-1.8.0 werkzeug-3.1.9

[notice] A new release of pip is available: 25.3 -> 26.2.1
[notice] To update, run: pip install --upgrade pip
==> Uploading build...
==> Uploaded in 5.2s. Compression took 1.3s
==> Build successful 🎉
==> Deploying...
==> Setting WEB_CONCURRENCY=1 by default, based on available CPUs in the instance
==> Running 'gunicorn app:app'
[2026-10-07 14:51:54 +0000] [58] [INFO] Starting gunicorn 26.2.0
[2026-10-07 14:51:54 +0000] [58] [INFO] Listening at: http://0.0.0.0:10000 (58)
[2026-10-07 14:51:54 +0000] [58] [INFO] Using worker: sync
[2026-10-07 14:51:54 +0000] [59] [INFO] Booting worker with pid: 59
127.0.0.1 - - [07/Oct/2026:14:51:54 +0000] "HEAD / HTTP/1.1" 200 0 "-" "Go-http-client/1.1"
[2026-10-07 14:51:55 +0000] [58] [INFO] Control socket listening at /opt/render/.gunicorn/gunicorn.ctl
==> Your service is live 🎉
127.0.0.1 - - [07/Oct/2026:14:51:59 +0000] "GET / HTTP/1.1" 200 5676 "-" "Go-http-client/2.0"
==> 
==> ///////////////////////////////////////////////////////////
==> 
==> Available at your primary URL https://btc-strike-ai-7.onrender.com
==> 
==> ///////////////////////////////////////////////////////////
127.0.0.1 - - [07/Oct/2026:14:52:41 +0000] "GET / HTTP/1.1" 200 5676 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1"
127.0.0.1 - - [07/Oct/2026:14:52:42 +0000] "GET /api/state?ts=1791384761279 HTTP/1.1" 200 638 "https://btc-strike-ai-7.onrender.com/" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1"
127.0.0.1 - - [07/Oct/2026:14:52:42 +0000] "GET /favicon.ico HTTP/1.1" 404 207 "https://btc-strike-ai-7.onrender.com/" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1"
127.0.0.1 - - [07/Oct/2026:14:52:47 +0000] "GET /api/state?ts=1791384766280 HTTP/1.1" 200 565 "https://btc-strike-ai-7.onrender.com/" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1"
127.0.0.1 - - [07/Oct/2026:14:52:52 +0000] "GET /api/state?ts=1791384771282 HTTP/1.1" 200 637 "https://btc-strike-ai-7.onrender.com/" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1"
127.0.0.1 - - [07/Oct/2026:14:52:52 +0000] "GET / HTTP/1.1" 200 5676 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1"
127
