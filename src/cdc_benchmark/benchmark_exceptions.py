class InvalidConfigException(Exception):
    def __init__(self, message: str = "Invalid config JSON formate") -> None:
        super().__init__(str)

    def __str__(self) -> str:
        return super().__str__()


class NoConfigFoundException(Exception):
    def __init__(self, message: str = "No config file were found") -> None:
        super().__init__(str)

    def __str__(self) -> str:
        return super().__str__()
