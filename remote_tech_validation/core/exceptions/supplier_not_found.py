class SupplierNotFound(Exception):
    def __init__(self, supplier_id):
        self.message = f"Supplier {supplier_id} not found"
        super().__init__(self.message)
