import jury

cases = [
    "cases/example",
    "cases/types/types",
    "cases/types/types_rewrite",
    "cases/cluster/sync",
    "cases/auth",
    "cases/auth_acl",
]


def main():
    j = jury.Jury(cases)
    j.run()


if __name__ == '__main__':
    main()
