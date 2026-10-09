# Contributing to ntk

Thanks for your interest in ntk! This page explains how you can help, and what we can't accept, so nobody spends time on something that won't be merged.

## The short version

- **Bug reports and feature ideas: yes, please.** Open an issue on GitHub.
- **Pull requests: not at the moment.** ntk is developed and maintained by the [ntail](https://ntail.io) team at Factual Tech AB, and we don't accept external code contributions. PRs will be closed, with thanks, without review.

We know that's unusual for an open-source project. ntk is small and moves quickly. Taking on outside changes would mean reviewing, maintaining and supporting them, and we'd rather spend that time making the tool good. This may change later.

Everything in this repository is Apache 2.0, so you're free to fork, patch and ship your own build.

## Reporting a bug

Open an issue and include:

1. The `ntk version` output and your OS.
2. The command you ran, with `--debug` if it's a connection or protocol problem. Mask anything sensitive: `--debug` logs Kafka requests, not passwords, but it does show hostnames and topic names.
3. What you expected, and what happened instead. Exact error text helps more than a description of it.
4. Your Kafka version, if you know it, and whether it's KRaft or ZooKeeper.

If you can reproduce it on the sandbox cluster in this repo (`docker compose up -d --wait`), say so; those are the quickest to fix.

## Suggesting a feature

Open an issue and tell us the problem you're trying to solve, not only the flag you'd like. "I need to copy a day of traffic into staging without the timestamps" is easier to act on than "add `--no-timestamps`". If ntk does something close already, mention what falls short.

Some things are deliberately out of scope. Message decoding and Schema Registry support, for example, aren't planned: ntk treats messages as bytes, on purpose.

## Questions and feedback

Issues are fine for questions too. If you just want to say ntk saved you an afternoon, we like hearing that as well.
