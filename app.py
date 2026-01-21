from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd


customers_df = pd.read_csv('tragerinc_customer_info.csv')
energy_df = pd.read_csv('tragerinc_energy_usage.csv')
tickets_df = pd.read_csv('tragerinc_support_tickets.csv')

app = FastAPI()

class Customer(BaseModel):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone_number: str
    address: str
    data_joined: str
    account_status: str

class EnergyUsage(BaseModel):
    customer_id: str
    date: str
    usage_kwh: float
    peak_demand_kwh: float
    total_charge: float
    energy_type: str

class SupportTicket(BaseModel):
    ticket_id: str
    customer_id: str
    issue_type: str
    ticket_status: str
    date_opened: str
    date_closed: Optional[str] = None
    resolution_method: Optional[str] = None



@app.get('/customers/{customer_id}', response_model=Customer)
def get_customer_info(customer_id: str):
    customer = customers_df[customers_df['Customer_ID'] == customer_id].iloc[0]
    return Customer(
        customer_id=customer['Customer_ID'],
        first_name=customer['First_Name'],
        last_name=customer['Last_Name'],
        email=customer['Email'],
        phone_number=customer['Phone_Number'],
        address=customer['Address'],
        data_joined=customer['Date_Joined'],
        account_status=customer['Account_Status']
    )


@app.get('/energy_usage/{customer_id}', response_model=List[EnergyUsage])
def get_energy_usage(customer_id: str):
    customer_energy = energy_df[energy_df['Customer_ID'] == customer_id]
    recent_energy_data = customer_energy[customer_energy['Date'] >= '2025-12-05']

    energy_data = []
    for index, row in recent_energy_data.iterrows():
        peak_demand_kwh = row.get('Peak_Demand_kWh', None)
        energy_data.append(
            EnergyUsage(
                customer_id=row['Customer_ID'],
                date=row['Date'],
                usage_kwh=row['Usage_kWh'],
                peak_demand_kwh=peak_demand_kwh,
                total_charge=row['Total_Charge'],
                energy_type=row['Energy_Type']
            )
        )
    
    return energy_data


@app.get("/support_tickets/{customer_id}", response_model=List[SupportTicket])
def get_support_tickets(customer_id: str):
    customer_tickets = tickets_df[tickets_df["Customer_ID"] == customer_id]

    customer_tickets['Date_Closed'] = customer_tickets['Date_Closed'].where(pd.notna(customer_tickets['Date_Closed']), None)
    customer_tickets['Resolution_Method'] = customer_tickets['Resolution_Method'].where(pd.notna(customer_tickets['Resolution_Method']), None)
    
    tickets_data = []
    for index, row in customer_tickets.iterrows():
        # date_closed = row['Date_Closed'] if pd.notna(row['Date_Closed']) else None
        # resolution_method = row['Resolution_Method'] if pd.notna(row['Resolution_Method']) else None

        if isinstance(date_closed, float) and pd.isna(date_closed):
            date_closed = None
        if isinstance(resolution_method, float) and pd.isna(resolution_method):
            resolution_method = None

        tickets_data.append(
            SupportTicket(
                ticket_id=row["Ticket_ID"],
                customer_id=row["Customer_ID"],
                issue_type=row["Issue_Type"],
                ticket_status=row["Ticket_Status"],
                date_opened=row["Date_Opened"],
                date_closed=row['Date_Closed'],
                resolution_method=row['Resolution_Method']
            )
        )
    
    return tickets_data