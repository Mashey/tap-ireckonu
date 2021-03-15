from dotenv import load_dotenv
import pytest
import os
from requests import Session

load_dotenv()

from tap_ireckonu.client import IreckonuClient

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

@pytest.fixture()
def client():
    yield IreckonuClient(client_id, client_secret)

def test_ireckonu_client_initialization(client):
    assert type(client._client) is Session

def test_fetch_created_bulk_company(client):
    response = client.fetch_created_bulk_company(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    company = response['Data'][0]
    assert "Id" in company
    assert "Name" in company
    assert "EmailAddress" in company
    assert "Comments" in company
    assert "VatNumber" in company
    assert "HotelCode" in company
    assert "Fax" in company
    assert "CreatedDateTime" in company
    assert "LastUpdateDateTime" in company
    assert "Addresses" in company
    assert "PhoneNumbers" in company
    assert "ExternalReferences" in company
    assert "Attribues" in company

def test_fetch_updated_bulk_company(client):
    response = client.fetch_updated_bulk_company(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    company = response['Data'][0]
    assert "Id" in company
    assert "Name" in company
    assert "EmailAddress" in company
    assert "Comments" in company
    assert "VatNumber" in company
    assert "HotelCode" in company
    assert "Fax" in company
    assert "CreatedDateTime" in company
    assert "LastUpdateDateTime" in company
    assert "Addresses" in company
    assert "PhoneNumbers" in company
    assert "ExternalReferences" in company
    assert "Attribues" in company

def test_fetch_created_bulk_folio(client):
    response = client.fetch_created_bulk_folio(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    folio = response['Data'][0]

    assert "Id" in folio
    assert "HotelCode" in folio
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

def test_fetch_updated_bulk_folio(client):
    response = client.fetch_updated_bulk_folio(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    folio = response['Data'][0]

    assert "Id" in folio
    assert "HotelCode" in folio
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

def test_fetch_created_bulk_house_account(client):
    response = client.fetch_created_bulk_house_account(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    house_account = response['Data'][0]

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
    
def test_fetch_updated_bulk_house_account(client):
    response = client.fetch_updated_bulk_house_account(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    house_account = response['Data'][0]

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

def test_fetch_created_bulk_person(client):
    response = client.fetch_created_bulk_person(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    person = response['Data'][0]

    assert "Id" in person
    assert "Title" in person
    assert "Names" in person
    assert "EmailAddresses" in person
    assert "LanguageCode" in person
    assert "CountryCode" in person
    assert "Gender" in person
    assert "DateOfBirth" in person
    assert "HotelCode" in person
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

def test_fetch_updated_bulk_person(client):
    response = client.fetch_updated_bulk_person(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    person = response['Data'][0]

    assert "Id" in person
    assert "Title" in person
    assert "Names" in person
    assert "EmailAddresses" in person
    assert "LanguageCode" in person
    assert "CountryCode" in person
    assert "Gender" in person
    assert "DateOfBirth" in person
    assert "HotelCode" in person
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

def test_fetch_created_bulk_reservation(client):
    response = client.fetch_created_bulk_reservation(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    reservation = response['Data'][0]   

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

def test_fetch_updated_bulk_reservation(client):
    response = client.fetch_updated_bulk_reservation(
        start_date="2021-03-15T20:31:41.988Z",
        end_date="2021-04-15T20:31:41.988Z",
        page=0,
        page_size=50
    )

    assert 'Count' in response
    assert 'Data' in response

    reservation = response['Data'][0]   
    
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