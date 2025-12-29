FROM ubuntu:20.04

# Set environment variables and prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements if you have any
# COPY requirements.txt .
# RUN pip3 install -r requirements.txt

# Copy your application files
# COPY . .

# Command to run when container starts
CMD ["bash"]