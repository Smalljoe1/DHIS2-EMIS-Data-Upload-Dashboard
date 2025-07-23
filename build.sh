#!/bin/bash
apt-get update && apt-get install -y \
  build-essential \
  python3-dev \
  meson \
  ninja-build \
  libatlas-base-dev \
  liblapack-dev \
  gfortran
pip install -r requirements.txt