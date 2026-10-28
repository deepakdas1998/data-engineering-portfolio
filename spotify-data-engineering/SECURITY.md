# Security Guidelines

This repository is for portfolio/interview demonstration.

Never commit real:

- Azure SQL passwords
- Azure Storage account keys
- SAS tokens
- Azure service principal secrets
- Databricks access tokens
- API keys
- Private keys
- `.env` files

Replace environment-specific values with placeholders such as:

```text
<YOUR_SQL_SERVER>
<YOUR_STORAGE_ACCOUNT>
<YOUR_DATABRICKS_WORKSPACE_URL>
<YOUR_SQL_USERNAME>
```

For real deployments, use Azure Key Vault, Databricks secret scopes,
managed identities, environment variables, or another appropriate
secret-management solution.

Workspace-specific URLs, personal emails, subscription IDs, tenant IDs,
and managed identity IDs should also be sanitized when they are not needed
to demonstrate the implementation.
