import json
import requests


class IreckonuClient:
    BASE_URL = "https://mint-mw-acc.ireckonu.com"
    BULK_API = "bapi/v1"
    COMPANY_ENPOINT = "company"
    FOLIO_ENDPOINT = "folio"
    HOUSE_ACCOUNT_ENDPOINT = "houseaccount"
    PERSON_ENDPOINT = "person"
    RESERVATION_ENDPOINT = "reservation"

    def __init__(self, client_id, client_secret):
        self._client = requests.Session()
        self._client.headers.update(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )

    def fetch_access_token(self, client_id, api_key):
        pass

    def fetch_created_bulk_company(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.COMPANY_ENPOINT}"
        payload = {
            "Criteria": self._created_criteria(start_date, end_date, page, page_size),
            "Filters": self.COMPANY_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_updated_bulk_company(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.COMPANY_ENPOINT}"
        payload = {
            "Criteria": self._updated_criteria(start_date, end_date, page, page_size),
            "Filters": self.COMPANY_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_created_bulk_folio(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.FOLIO_ENDPOINT}"
        payload = {
            "Criteria": self._created_criteria(start_date, end_date, page, page_size),
            "Filters": self.FOLIO_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_updated_bulk_folio(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.FOLIO_ENDPOINT}"
        payload = {
            "Criteria": self._updated_criteria(start_date, end_date, page, page_size),
            "Filters": self.FOLIO_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_created_bulk_house_account(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.HOUSE_ACCOUNT_ENDPOINT}"
        payload = {
            "Criteria": self._created_criteria(start_date, end_date, page, page_size),
            "Filters": self.HOUSE_ACCOUNT_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_updated_bulk_house_account(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.HOUSE_ACCOUNT_ENDPOINT}"
        payload = {
            "Criteria": self._updated_criteria(start_date, end_date, page, page_size),
            "Filters": self.HOUSE_ACCOUNT_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_created_bulk_person(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.PERSON_ENDPOINT}"
        payload = {
            "Criteria": self._created_criteria(start_date, end_date, page, page_size),
            "Filters": self.PERSON_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_updated_bulk_person(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.PERSON_ENDPOINT}"
        payload = {
            "Criteria": self._updated_criteria(start_date, end_date, page, page_size),
            "Filters": self.PERSON_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_created_bulk_reservation(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.PERSON_ENDPOINT}"
        payload = {
            "Criteria": self._created_criteria(start_date, end_date, page, page_size),
            "Filters": self.RESERVATION_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    def fetch_updated_bulk_reservation(
        self, start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        url = f"{self.BASE_URL}/{self.BULK_API}/{self.RESERVATION_ENDPOINT}"
        payload = {
            "Criteria": self._updated_criteria(start_date, end_date, page, page_size),
            "Filters": self.RESERVATION_FILTERS,
        }
        return self._client.post(url, data=payload).json()

    @staticmethod
    def _created_criteria(
        start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        return {
            "Start": start_date,
            "End": end_date,
            "Type": "Created",
            "Take": page_size,
            "Skip": page * page_size,
        }

    @staticmethod
    def _updated_criteria(
        start_date: str, end_date: str, page: int, page_size: int
    ) -> dict:
        return {
            "Start": start_date,
            "End": end_date,
            "Type": "Updated",
            "Take": page_size,
            "Skip": page * page_size,
        }

    COMPANY_FILTERS = {
        "IncludeExternalReferences": True,
        "IncludeAddresses": True,
        "IncludePhoneNumbers": True,
        "IncludeEmailAddresses": True,
    }

    FOLIO_FILTERS = {"IncludeInvoiceLines": True, "IncludeAddress": True}

    HOUSE_ACCOUNT_FILTERS = {
        "IncludeExternalReferences": True,
        "IncludePrimaryPerson": True,
        "PrimaryPerson": {
            "IncludeEmailAddresses": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeAttributes": True,
            "IncludeDataConsents": True,
            "IncludePaymentMethods": True,
            "IncludeStaySummaries": True,
            "IncludeDocuments": True,
            "IncludeGlobalStay": True,
            "IncludeExternalReferences": True,
            "IncludeLoyaltyLevels": True,
            "IncludePreferences": True,
            "IncludeCompany": True,
            "Company": {
                "IncludeExternalReferences": True,
                "IncludeAddresses": True,
                "IncludePhoneNumbers": True,
                "IncludeEmailAddresses": True,
            },
        },
        "IncludeContactPerson": True,
        "ContactPerson": {
            "IncludeEmailAddresses": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeAttributes": True,
            "IncludeDataConsents": True,
            "IncludePaymentMethods": True,
            "IncludeStaySummaries": True,
            "IncludeDocuments": True,
            "IncludeGlobalStay": True,
            "IncludeExternalReferences": True,
            "IncludeLoyaltyLevels": True,
            "IncludePreferences": True,
            "IncludeCompany": True,
            "Company": {
                "IncludeExternalReferences": True,
                "IncludeAddresses": True,
                "IncludePhoneNumbers": True,
                "IncludeEmailAddresses": True,
            },
        },
        "IncludeCompany": True,
        "Company": {
            "IncludeExternalReferences": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeEmailAddresses": True,
        },
        "IncludeAgency": True,
        "Agency": {
            "IncludeExternalReferences": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeEmailAddresses": True,
        },
        "IncludeSourceCompany": True,
        "SourceCompany": {
            "IncludeExternalReferences": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeEmailAddresses": True,
        },
    }

    PERSON_FILTERS = {
        "IncludeEmailAddresses": True,
        "IncludeAddresses": True,
        "IncludePhoneNumbers": True,
        "IncludeAttributes": True,
        "IncludeDataConsents": True,
        "IncludePaymentMethods": True,
        "IncludeStaySummaries": True,
        "IncludeDocuments": True,
        "IncludeGlobalStay": True,
        "IncludeExternalReferences": True,
        "IncludeLoyaltyLevels": True,
        "IncludePreferences": True,
        "IncludeCompany": True,
        "Company": {
            "IncludeExternalReferences": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeEmailAddresses": True,
        },
    }

    RESERVATION_FILTERS = {
        "IncludeBooker": True,
        "Booker": {
            "IncludeEmailAddresses": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeAttributes": True,
            "IncludeDataConsents": True,
            "IncludePaymentMethods": True,
            "IncludeStaySummaries": True,
            "IncludeDocuments": True,
            "IncludeGlobalStay": True,
            "IncludeExternalReferences": True,
            "IncludeLoyaltyLevels": True,
            "IncludePreferences": True,
            "IncludeCompany": True,
            "Company": {
                "IncludeExternalReferences": True,
                "IncludeAddresses": True,
                "IncludePhoneNumbers": True,
                "IncludeEmailAddresses": True,
            },
        },
        "IncludePrimaryGuest": True,
        "PrimaryGuest": {
            "IncludeEmailAddresses": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeAttributes": True,
            "IncludeDataConsents": True,
            "IncludePaymentMethods": True,
            "IncludeStaySummaries": True,
            "IncludeDocuments": True,
            "IncludeGlobalStay": True,
            "IncludeExternalReferences": True,
            "IncludeLoyaltyLevels": True,
            "IncludePreferences": True,
            "IncludeCompany": True,
            "Company": {
                "IncludeExternalReferences": True,
                "IncludeAddresses": True,
                "IncludePhoneNumbers": True,
                "IncludeEmailAddresses": True,
            },
        },
        "IncludeOtherLinkedPersons": True,
        "OtherLinkedPersons": {
            "IncludeEmailAddresses": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeAttributes": True,
            "IncludeDataConsents": True,
            "IncludePaymentMethods": True,
            "IncludeStaySummaries": True,
            "IncludeDocuments": True,
            "IncludeGlobalStay": True,
            "IncludeExternalReferences": True,
            "IncludeLoyaltyLevels": True,
            "IncludePreferences": True,
            "IncludeCompany": True,
            "Company": {
                "IncludeExternalReferences": True,
                "IncludeAddresses": True,
                "IncludePhoneNumbers": True,
                "IncludeEmailAddresses": True,
            },
        },
        "IncludeContacts": True,
        "IncludeRatePlan": True,
        "IncludeExtraServices": True,
        "IncludeExternalReferences": True,
        "IncludePaymentInformations": True,
        "IncludePaymentMethods": True,
        "IncludeCompany": True,
        "IncludeAttributes": True,
        "IncludePreferences": True,
        "Company": {
            "IncludeExternalReferences": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeEmailAddresses": True,
        },
        "IncludeAgency": True,
        "Agency": {
            "IncludeExternalReferences": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeEmailAddresses": True,
        },
        "IncludeSourceCompany": True,
        "SourceCompany": {
            "IncludeExternalReferences": True,
            "IncludeAddresses": True,
            "IncludePhoneNumbers": True,
            "IncludeEmailAddresses": True,
        },
        "Extension": {
            "IncludeRateplans": True,
            "IncludeExtras": True,
            "IncludeUnitTypes": True,
        },
    }
