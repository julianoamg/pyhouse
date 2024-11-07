import math


class HousePaginator:
    page = 0
    has_next = False
    has_previous = False

    def __init__(self, query, page, per_page):
        self.query = query
        self.page = int(page)
        self.per_page = int(per_page)
        self.count = self.query.count()
        self.query = self.query.max(self.per_page).offset((self.page - 1) * self.per_page)
        self.total_pages = int(math.ceil(self.count / self.per_page))
        self.has_next = self.page < self.total_pages
        self.has_previous = page > 1

    def as_dict(self):
        return {
            "current_page": self.page,
            "total_pages": self.total_pages,
            "has_previous": self.has_previous,
            "has_next": self.has_next,
            "total_items": self.count,
            "per_page": self.per_page
        },
