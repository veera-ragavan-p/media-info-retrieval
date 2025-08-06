class ProfileNotFound(Exception):
    def __init__(self, profile_id):
        self.message = f"Profile {profile_id} not found for the supplier"
        super().__init__(self.message)
