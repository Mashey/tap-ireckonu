from dotenv import load_dotenv
import pytest
import os
from requests import Session
from datetime import datetime, timedelta

load_dotenv()

from tap_ireckonu.client import IreckonuClient

config = {
    "USERNAME": "mashyusr",
    "PASSWORD": os.getenv("PASSWORD"),
    "CLIENT_ID": os.getenv("CLIENT_ID"),
    "CLIENT_SECRET": os.getenv("CLIENT_SECRET"),
}


@pytest.fixture()
def client():
    client = IreckonuClient(config)
    client.fetch_access_token()

    yield client


def test_fetch_bulk_company(client):
    start_date = "2020-10-30"
    end_date = "2020-10-31"
    hotel_codes = []

    today = datetime.strftime(datetime.today(), "%Y-%m-%d")

    while end_date != today:
        response = client.fetch_bulk_company(
            start_date=start_date, end_date=end_date, page=0, page_size=50
        )

        assert "Count" in response
        assert "Data" in response

        print(f'{start_date} - Response: {response["Count"]}')
        if len(response["Data"]) > 0:
            print("Testing Company Data")
        for company in response["Data"]:
            assert "Id" in company
            assert "Name" in company
            assert "EmailAddress" in company
            assert "Comments" in company
            assert "VatNumber" in company
            assert "HotelCode" in company
            hotel_codes.append(company["HotelCode"])
            assert "Fax" in company
            assert "CreatedDateTime" in company
            assert "LastUpdateDateTime" in company
            assert "Addresses" in company
            assert "PhoneNumbers" in company
            assert "ExternalReferences" in company
            assert "Attribues" in company

        start_date = end_date
        end_date = datetime.strftime(
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)), "%Y-%m-%d"
        )

    print(f"{set(hotel_codes)}")


def test_fetch_bulk_folio(client):
    start_date = "2020-10-30"
    end_date = "2020-10-31"

    hotel_codes = []

    today = datetime.strftime(datetime.today(), "%Y-%m-%d")

    while end_date != today:
        response = client.fetch_bulk_folio(
            start_date=start_date, end_date=end_date, page=0, page_size=50
        )

        assert "Count" in response
        assert "Data" in response
        print(f'{start_date} - Response: {response["Count"]}')
        if len(response["Data"]) > 0:
            print("Testing Folio Data")
        for folio in response["Data"]:
            assert "Id" in folio
            assert "HotelCode" in folio
            hotel_codes.append(folio["HotelCode"])
            assert "ReservationId" in folio
            assert "Total" in folio
            assert "VatTotal" in folio
            assert "InvoiceNumber" in folio
            assert "AdditionalInvoiceNumber" in folio
            assert "PrimaryPersonId" in folio
            assert "AgencyId" in folio
            assert "CompanyId" in folio
            assert "SourceCompanyId" in folio
            assert "System" in folio
            assert "Status" in folio
            assert "Address" in folio
            assert "Lines" in folio
            assert "CreatedDateTime" in folio
            assert "LastChangedDateTime" in folio

        start_date = end_date
        end_date = datetime.strftime(
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)), "%Y-%m-%d"
        )

    print(f"{set(hotel_codes)}")


def test_fetch_bulk_person(client):
    start_date = "2020-10-30"
    end_date = "2020-10-31"

    hotel_codes = []

    today = datetime.strftime(datetime.today(), "%Y-%m-%d")

    while end_date != today:
        response = client.fetch_bulk_person(
            start_date=start_date, end_date=end_date, page=0, page_size=50
        )

        assert "Count" in response
        assert "Data" in response

        print(f'{start_date} - Response: {response["Count"]}')
        if len(response["Data"]) > 0:
            print("Testing Person Data")
        for person in response["Data"]:
            assert "Id" in person
            assert "Title" in person
            assert "Names" in person
            assert "EmailAddresses" in person
            assert "LanguageCode" in person
            assert "CountryCode" in person
            assert "Gender" in person
            assert "DateOfBirth" in person
            assert "HotelCode" in person
            hotel_codes.append(person["HotelCode"])
            assert "Source" in person
            assert "JobTitle" in person
            assert "IsMember" in person
            assert "NewsletterSubscription" in person
            assert "Company" in person
            assert "CompanyName" in person
            assert "ExternalReferences" in person
            assert "Attributes" in person
            assert "Addresses" in person
            assert "PhoneNumbers" in person
            assert "DataConsents" in person
            assert "PaymentMethods" in person
            assert "Preferences" in person
            assert "LoyaltyLevels" in person
            assert "StaySummaries" in person
            assert "Documents" in person
            assert "GlobalStay" in person
            assert "LastChangedBySystem" in person
            assert "CreatedDateTime" in person
            assert "LastUpdateDateTime" in person

        start_date = end_date
        end_date = datetime.strftime(
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)), "%Y-%m-%d"
        )

    print(f"{set(hotel_codes)}")


def test_fetch_bulk_house_account(client):

    hotel_codes = [
        "800TWR",
        "BLVD",
        "BUCKLER",
        "CCAMINN",
        "CAOBA",
        "CHESTNUT",
        "GRAND",
        "GUTHCOKE",
        "HENCLAY",
        "MIDPLACE",
        "PHILIP",
        "RESERVE",
        "RIVERWALK",
        "VILLCENTER",
        "VILLROW",
        "BRIGGS",
        "Hatchery",
        "HighCherry",
        "SoCo",
        "Muze",
    ]

    today = datetime.strftime(datetime.today(), "%Y-%m-%d")

    for hotel_code in hotel_codes:
        start_date = "2020-10-30"
        end_date = "2020-10-31"
        while end_date != today:
            response = client.fetch_bulk_house_account(
                hotel_code=hotel_code,
                start_date=start_date,
                end_date=end_date,
                page=0,
                page_size=50,
            )

            assert "Count" in response
            assert "Data" in response

            print(
                f'{start_date} - Response: {response["Count"]} - Hotel Code: {hotel_code}'
            )
            if len(response["Data"]) > 0:
                print("Testing House Account Data")
            for house_account in response["Data"]:
                assert "Id" in house_account
                assert "CompanyCode" in house_account
                assert "MarketCode" in house_account
                assert "Name" in house_account
                assert "Comments" in house_account
                assert "SourceCode" in house_account
                assert "VirtualRoomCode" in house_account
                assert "HotelCode" in house_account
                assert "ExternalReferences" in house_account
                assert "ContactPerson" in house_account
                assert "PrimaryPerson" in house_account
                assert "Agency" in house_account
                assert "Company" in house_account
                assert "SourceCompany" in house_account

            start_date = end_date
            end_date = datetime.strftime(
                (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)),
                "%Y-%m-%d",
            )


def test_fetch_bulk_reservation(client):
    hotel_codes = [
        "800TWR",
        "BLVD",
        "BUCKLER",
        "CCAMINN",
        "CAOBA",
        "CHESTNUT",
        "GRAND",
        "GUTHCOKE",
        "HENCLAY",
        "MIDPLACE",
        "PHILIP",
        "RESERVE",
        "RIVERWALK",
        "VILLCENTER",
        "VILLROW",
        "BRIGGS",
        "Hatchery",
        "HighCherry",
        "SoCo",
        "Muze",
    ]

    today = datetime.strftime(datetime.today(), "%Y-%m-%d")

    for hotel_code in hotel_codes:
        start_date = "2020-10-30"
        end_date = "2020-10-31"
        while end_date != today:
            response = client.fetch_bulk_reservation(
                hotel_code=hotel_code,
                start_date=start_date,
                end_date=end_date,
                page=0,
                page_size=50,
            )

            assert "Count" in response
            assert "Data" in response

            print(
                f'{start_date} - Response: {response["Count"]} - Hotel Code: {hotel_code}'
            )
            if len(response["Data"]) > 0:
                print("Testing Reservation Data")
            for reservation in response["Data"]:
                assert "Id" in reservation
                assert "SubReservations" in reservation
                assert "ArrivalDate" in reservation
                assert "DepartureDate" in reservation
                assert "HotelCode" in reservation
                assert "UnitCode" in reservation
                assert "UnitTypeCode" in reservation
                assert "RequestedUnitTypeCode" in reservation
                assert "Status" in reservation
                assert "NumberOfPersons" in reservation
                assert "NumberOfChildren" in reservation
                assert "NumberOfInfants" in reservation
                assert "NumberOfUnits" in reservation
                assert "NumberOfNights" in reservation
                assert "Comments" in reservation
                assert "Source" in reservation
                assert "SubSource" in reservation
                assert "GroupCode" in reservation
                assert "GuaranteeTypeCode" in reservation
                assert "MarketCode" in reservation
                assert "PromoCode" in reservation
                assert "ActualArrivalDate" in reservation
                assert "ActualDepartureDate" in reservation
                assert "Booker" in reservation
                assert "PrimaryGuest" in reservation
                assert "RatePlan" in reservation
                assert "Agency" in reservation
                assert "Company" in reservation
                assert "SourceCompany" in reservation
                assert "ExtraServices" in reservation
                assert "Preferences" in reservation
                assert "ExternalReferences" in reservation
                assert "OtherLinkedPersons" in reservation
                assert "Contacts" in reservation
                assert "CreatedDateTime" in reservation
                assert "LastChangedDateTime" in reservation
                assert "BookingDateTime" in reservation
                assert "CancellationDateTime" in reservation
                assert "PaymentInformations" in reservation
                assert "PaymentMethods" in reservation
                assert "Attributes" in reservation
                assert "SharerReservationId" in reservation

            start_date = end_date
            end_date = datetime.strftime(
                (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)),
                "%Y-%m-%d",
            )
