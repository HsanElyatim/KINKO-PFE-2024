FROM quay.io/astronomer/astro-runtime:11.5.0

# Switch to root to install Firefox and other dependencies
USER root

RUN apt-get update                             \
    && apt-get install -y --no-install-recommends \
    ca-certificates curl firefox-esr           \
    && rm -fr /var/lib/apt/lists/*                \
    && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz | tar xz -C /usr/local/bin \
    && apt-get purge -y ca-certificates curl

# Set PYTHONPATH
ENV PYTHONPATH="/usr/local/airflow/dags:/usr/local/airflow/dags/scrapers:$PYTHONPATH"