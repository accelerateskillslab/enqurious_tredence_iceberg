# Iceberg AWS Infra Lab

## 1. Create AWS Access Key

In AWS Console, create an access key for your lab user.

## 2. Clone Repo

```cmd
git clone https://github.com/accelerateskillslab/enqurious_tredence_iceberg.git
cd enqurious_tredence_iceberg
```

## 3. Create `.env`

```cmd
copy user.env.example .env
```

Fill all values.

## 4. Run Setup

```cmd
scripts\setup_lab.cmd
```

This installs/checks:

- Python 3.11
- `.venv`
- AWS CLI
- Terraform

## 5. Create Infra

```cmd
scripts\terraform.cmd plan
scripts\terraform.cmd apply
```

## 6. Destroy Infra

```cmd
scripts\terraform.cmd destroy
```

## Notes

- Use VS Code Command Prompt terminal.
- Do not commit `.env`.
- Region: `us-east-1`.
