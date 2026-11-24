# Working together

Use one shared repository. Each member develops their agreed part on a separate
branch and follows the shared data and executable-job contracts.

1. Pull the latest `main` and create a short-lived branch, e.g. `feature/silver-hudi`.
2. Keep changes focused and update the relevant README and data contract.
3. Run `python -m unittest discover -s tests -v` and `docker compose config --quiet`.
4. For changes affecting the pipeline, also run the integration check in the README.
5. Push the branch and open a pull request. Review it before merging into `main`.

Never commit `.env`, downloaded archives, Docker volume data, passwords, or API tokens.
Each member should use their own Git account so contributions remain attributable.
Do not change a topic, path, schema, or command without updating its callers and docs.
