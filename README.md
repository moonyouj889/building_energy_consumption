# Building Energy Consumption

## Introduction

This is a data engineering project based on the Google Cloud Platform, specifically utilizing the Pub/Sub-Dataflow-BigQuery severless stream processing. There are two problems that this project aims to solve:

1. Provide an API for data scientists and cab dispatchers, for analyzing long term trends in cab behavior w.r.t metrics such as average pickups, dropoffs, occupancy, miles travelled etc.
1. Enable a framework for real-time monitoring of cab locations, so that a user can know the unoccupied cabs across a city and zoom in on a specific neighborhood to spot and catch available cabs nearest to them.`

Parallel processing pipelines of streaming data

## Table of Contents

1. [Architecture](#architecture)
1. [Getting Started](#getting-started)
   1. [Installing Dependencies](#installing-dependencies)
   1. [Development Environment](#development-environment)
1. [Authors](#authors)
1. [Screenshots](#screenshots)
1. [Styling](#styling)
1. [Contributing](#contributing)
1. [Licensing](#licensing)

## Architecture

## Getting Started

### Installing Dependencies

You must install Docker to be able to run this application. Please reference [Docker](https://www.docker.com/) on the installation procedure.

### Development Environment

To start up the multi container application, from within the root directory:

```sh
docker-compose up
```

To check the containers are running:

```sh
docker-compose ps
```
