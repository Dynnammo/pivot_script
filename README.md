# pivot_script
This is a project of the company [Open Source Politics](https://opensourcepolitics.eu), based on the [python-app template](https://github.com/OpenSourcePolitics/python-app/).

Write the request from a Metabase query that linearize answers of a Decidim survey

## Getting started
Requirements : 
- [**Python 3**](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org) (TL;DR run `curl -sSL https://install.python-poetry.org | python3 -` should work)
- A [Git Guardian](https://dashboard.gitguardian.com/auth/login) account. You can create one by using GitHub SSO. 

1. Clone repository : `git clone https://github.com/OpenSourcePolitics/pivot_script.git`)
2. Setup repository : `./script/setup`. Launching the script will
    1. Check requirements
    2. Ask you to enter needed values for project name and other informations
    3. Install dependencies
    4. Launch the virtual environment to get you started
    5. Setup GGShield for secret scan

### Development scripts
- `./script/lint` to run flake8 against your code
- `./script/test` to run pytest against your code

### Hooks
This repository is setup by design with [`pre-commit`](https://pre-commit.com/), a tool that ease the installation of git hooks. By default, hooks currently enabled are:
- linting : using [flake8](https://github.com/pycqa/flake8)
- check for secrets : using [git guardian](https://docs.gitguardian.com/ggshield-docs/integrations/git-hooks/pre-commit)

If you want to modify or add your own hooks, please check [the pre-commit documentation](https://pre-commit.com/)

## Contribute
- [Create an issue](https://github.com/OpenSourcePolitics/pivot_script/issues) to report a bug/ask for a new feature
- [Fork this project](https://github.com/OpenSourcePolitics/pivot_script/issues) to make your changes and make a PR

## License
This software is licensed under the GNU AGPLv3, which states that **you can use, modify and redistribute this software as long as you publish the modifications under the same license.**
For more information, check [here](https://www.gnu.org/licenses/agpl-3.0.html)