#!/usr/bin/env bash

#
# Publish site to GitHub Pages
#
# How to run in Docker:
#
#   docker run --rm -it \
#	    -v "${PWD}/docs":/code/docs \
#	    -v "${PWD}/.circleci":/code/.circleci \
#	    -v ~/.ssh:/root/.ssh \
#	    -v ~/.gitconfig:/root/.gitconfig \
#	    -w /code \
#	    bitnami/git:latest /code/docs/publish-site.sh
#

set -ev

# Install Git
apt-get -y update
apt-get -y install git

echo "# Clone client and switch to branch for GH-Pages"
git clone -b gh-pages git@github.com:InfluxCommunity/influxdb3-csharp.git /code/influxdb3-csharp

echo "# Remove old pages"
rm -r /code/influxdb3-csharp/*

echo "# Copy new docs"
cp -Rf /code/docs/_site/* /code/influxdb3-csharp/

echo "# Copy CircleCI"
cp -R /code/.circleci/ /code/influxdb3-csharp/

echo "# Deploy site"
cd /code/influxdb3-csharp/ || exit
git add -f .
git -c commit.gpgsign=false commit -m "Pushed the latest Docs to GitHub pages [skip CI]"
git push -fq origin gh-pages
