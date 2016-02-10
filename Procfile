worker: python -u taskclustercoalesce/listener.py
web: gunicorn --config=config/gunicorn.py taskclustercoalesce.web:app --log-file -
