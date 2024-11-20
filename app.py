import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st
import time
import pandas as pd
import os

# Fonction pour se connecter à l'API Facebook et récupérer les leads avec pagination
def get_facebook_leads(access_token, ad_id, limit=100):
    leads_data = []
    url = f"https://graph.facebook.com/v17.0/{ad_id}/leads?access_token={access_token}&limit=100"

    while limit > 0:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            leads_data.extend(data.get('data', []))

            # Décrémenter le nombre de leads restants à récupérer
            limit -= len(data.get('data', []))

            # Vérifier s'il y a une page suivante
            if 'paging' in data and 'next' in data['paging']:
                url = data['paging']['next']
                time.sleep(1)  # Pause d'une seconde pour éviter les limitations de l'API
            else:
                break  # Plus de pages disponibles, sortir de la boucle
        else:
            st.error(f"Erreur lors de la récupération des leads : {response.status_code}")
            break

    return leads_data[:limit]  # Retourne uniquement le nombre de leads souhaité

# Fonction pour se connecter à Google Sheets via l'API et accéder à la feuille "resiliation"
def connect_to_google_sheets(sheet_id):
    try:
        # Charger les informations d'identification depuis les variables d'environnement
        json_credentials = os.getenv('GOOGLE_CREDENTIALS_JSON', '{}')
        with open('temp_credentials.json', 'w') as f:
            f.write(json_credentials)

        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('temp_credentials.json', scope)
        client = gspread.authorize(creds)
        
        # Accéder à la feuille spécifique "resiliation"
        sheet = client.open_by_key(sheet_id).worksheet("resiliation")
        return sheet
    except Exception as e:
        st.error(f"Erreur lors de la connexion à Google Sheets : {e}")
        return None
    finally:
        # Supprimer le fichier temporaire
        if os.path.exists('temp_credentials.json'):
            os.remove('temp_credentials.json')

# Fonction pour convertir les données brutes en dictionnaire lisible
def create_lead_dict(lead):
    lead_dict = {}
    lead_dict['created_time'] = lead.get('created_time', '')
    lead_dict['id'] = lead.get('id', '')

    for field in lead.get('field_data', []):
        field_name = field['name'].lower().replace(' ', '_')
        lead_dict[field_name] = field['values'][0] if field['values'] else ''
    
    return lead_dict

# Fonction pour transformer les leads en DataFrame Pandas
def leads_to_dataframe(leads):
    leads_dict_list = [create_lead_dict(lead) for lead in leads]
    return pd.DataFrame(leads_dict_list)

# Fonction pour insérer les leads dans Google Sheets
def insert_leads_to_sheets(worksheet, leads_df):
    try:
        if leads_df.empty:
            st.info("Aucun lead à insérer.")
            return

        data = leads_df.values.tolist()

        # Ajouter l'entête si la feuille est vide
        if worksheet.row_count == 1:
            worksheet.append_row(leads_df.columns.tolist())

        batch_size = 50
        for i in range(0, len(data), batch_size):
            worksheet.append_rows(data[i:i + batch_size])
            st.success(f"{min(i + batch_size, len(data))} leads insérés dans Google Sheets.")
            time.sleep(30)  # Délai pour éviter les limites de quota
        
    except Exception as e:
        st.error(f"Erreur lors de l'insertion des leads dans Google Sheets : {e}")

# Interface utilisateur avec Streamlit
def main():
    st.title("Analyse des Leads Facebook et Intégration Google Sheets")
    
    # Charger les informations depuis les variables d'environnement
    access_token = os.getenv('FACEBOOK_ACCESS_TOKEN', '')
    ad_id = os.getenv('FACEBOOK_AD_ID', '')
    sheet_id = os.getenv('GOOGLE_SHEET_ID', '')
    
    st.write("ID de l'annonce Facebook :", ad_id)
    st.write("ID de la feuille Google Sheets :", sheet_id)
    
    limit = st.number_input("Nombre de leads à récupérer", min_value=1, step=1)
    
    # Récupérer les leads
    if st.button("Récupérer les leads"):
        if access_token and ad_id and sheet_id:
            leads = get_facebook_leads(access_token, ad_id, limit)
            st.write(f"{len(leads)} leads récupérés.")

            leads_df = leads_to_dataframe(leads)
            if not leads_df.empty:
                st.write("Données structurées :")
                st.dataframe(leads_df)

                st.session_state['leads_df'] = leads_df
            else:
                st.write("Aucune donnée à afficher.")
        else:
            st.warning("Veuillez vérifier vos identifiants.")

    # Insérer les données dans Google Sheets
    if st.button("Insérer les leads dans Google Sheets"):
        leads_df = st.session_state.get('leads_df', None)
        if leads_df is not None:
            worksheet = connect_to_google_sheets(sheet_id)
            if worksheet:
                insert_leads_to_sheets(worksheet, leads_df)

    # Ajout de filtres dynamiques
    if 'leads_df' in st.session_state:
        leads_df = st.session_state['leads_df']
        st.subheader("Filtres dynamiques")

        filtered_df = leads_df.copy()
        for column in leads_df.columns:
            unique_values = leads_df[column].unique()
            selected_values = st.multiselect(f"Sélectionnez les valeurs pour {column}:", unique_values, default=unique_values)
            filtered_df = filtered_df[filtered_df[column].isin(selected_values)]

        st.write("Données filtrées :")
        st.dataframe(filtered_df)

if __name__ == "__main__":
    main()
