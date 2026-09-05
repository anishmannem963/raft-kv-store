# Interactive project website

This directory contains the interviewer-facing demonstration site for the Raft key-value store.

## Features

- interactive leader crash, network partition, lagging follower, and recovery simulation
- quorum write visualization and event stream
- architecture, write path, persistence, snapshot, and compaction explanations
- verified benchmark and 50-scenario fault-matrix evidence
- interview questions with concise technical answers

The browser simulation explains the behavior of the released system; it does not directly control a live Docker cluster.

## Run locally

Open `index.html` in a browser, or serve this directory with any static file server.

## Deploy with Netlify

The repository-level `netlify.toml` publishes this directory as a static site. In Netlify:

1. Choose **Add new site → Import an existing project**.
2. Connect this GitHub repository.
3. Leave the build command empty.
4. Netlify will use `website` as the publish directory from `netlify.toml`.
5. Deploy the site.

Future commits to the selected production branch will trigger automatic Netlify deployments.
