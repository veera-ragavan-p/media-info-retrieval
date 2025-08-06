class CorruptedFile(Exception):
    def __init__(self):
        self.message = "File is corrupted"
        super().__init__(self.message)
