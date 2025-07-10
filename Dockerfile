FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

COPY requirements.txt .
COPY setup.py .
COPY nps_intercom.py .

# Install Python dependencies and upgrade pip
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Set timezone for correct date handling
ENV TZ=Europe/Moscow

# Entrypoint configuration for Flex Template launcher
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/nps_intercom.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"