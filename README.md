 # DHIS2 EMIS Data Upload Dashboard
 
 ## Overview
 
 The DHIS2 EMIS Data Upload Dashboard is a web-based application designed to monitor and manage data uploads from schools to the DHIS2 EMIS (Education Management Information System) platform. It provides features to fetch datasets, user information, generate reports, track validation violations, and visualize upload statuses, enhancing data quality and user accountability.
 ## Features
 
- Fetch and display datasets and user data from DHIS2
- Generate comprehensive reports on school upload statuses
- Track and display validation rule violations
- Visualize statistics with interactive charts and quick stats
- Filter and sort data by LGA, school name, and other fields
- Toggle table visibility for a cleaner UI
- Export reports and charts as Excel files or images
- Responsive design with a modern interface
 
 ## Prerequisites
 
- Python 3.8+
- Node.js (for frontend dependencies, if needed)
- Virtual environment (recommended)
- DHIS2 API access with a valid API token
 
 ## Installation
 
 ### Backend Setup
 
 Clone the repository:
 
 ```sh
 git clone <repository-url>
 cd DHIS2-EMIS-Data-Upload-Dashboard
 ```
 
 Create a virtual environment and activate it:
 
 ```sh
 python -m venv venv
 # Windows
 .\venv\Scripts\activate
 # MacOS/Linux
 # source venv/bin/activate
 ```
 
 Install dependencies:
 
 ```sh
 pip install -r requirements.txt
 ```
 
 Create a `.env` file in the project root with your DHIS2 API token:
 
 ```env
 DHIS2_API_TOKEN=your_d2pat_token_here
 ```
 
 Run the application:
 
 ```sh
 python app.py
 ```
 
 ### Frontend Setup
 
 The frontend is embedded in `index.html` and uses CDN-hosted libraries. No additional setup is required beyond running the backend.
 
 ## Usage
 
- Access the dashboard at [http://localhost:5000/](http://localhost:5000/) after starting the server
- Select a state from the dropdown to filter data
- Use the buttons to fetch datasets, users, reports, and violations
- Apply filters or sort data as needed
- Toggle tables to manage screen space
- Download reports or charts using the provided links
 
 ## Configuration
 
- `BASE_URL`: Set to `https://emis.dhis2nigeria.org.ng/dhis/api` in `app.py`
- `HEADERS`: Configured with the API token from the `.env` file
- `DATASET_UIDS`: Defined in `app.py` for specific datasets to monitor
 
 ## Development
 
- Modify `app.py` for backend logic changes
- Update `index.html` for UI adjustments using React and Tailwind CSS
- Add new dependencies to `requirements.txt` as needed
- Test changes locally before deployment
 
 ## Contributing
 
 1. Fork the repository
 2. Create a feature branch (`git checkout -b feature-name`)
 3. Commit changes (`git commit -m "Add feature-name"`)
 4. Push to the branch (`git push origin feature-name`)
 5. Open a pull request
 
 ## License
 
 This project is licensed under the MIT License - see the LICENSE file for details.
 
 ## Acknowledgments
 
 Built with Flask, React, and Tailwind CSS.
 Thanks to the DHIS2 community for the API and documentation.

Fetch and display datasets and user data from DHIS2.
Generate comprehensive reports on school upload statuses.
Track and display validation rule violations.
Visualize statistics with interactive charts and quick stats.
Filter and sort data by LGA, school name, and other fields.
Toggle table visibility for a cleaner UI.
Export reports and charts as Excel files or images.
Responsive design with a modern interface.

Prerequisites

Python 3.8+
Node.js (for frontend dependencies, if needed)
Virtual environment (recommended)
DHIS2 API access with a valid API token

Installation
Backend Setup

Clone the repository:git clone <repository-url>
cd DHIS2-EMIS-Data-Upload-Dashboard


Create a virtual environment and activate it:python -m venv venv
.\venv\Scripts\activate  # Windows
# source venv/bin/activate  # MacOS/Linux


Install dependencies:pip install -r requirements.txt


Create a .env file in the project root with your DHIS2 API token:DHIS2_API_TOKEN=your_d2pat_token_here


Run the application:python app.py



Frontend Setup
The frontend is embedded in index.html and uses CDN-hosted libraries. No additional setup is required beyond running the backend.
Usage

Access the dashboard at http://localhost:5000/ after starting the server.
Select a state from the dropdown to filter data.
Use the buttons to fetch datasets, users, reports, and violations.
Apply filters or sort data as needed.
Toggle tables to manage screen space.
Download reports or charts using the provided links.

Configuration

BASE_URL: Set to https://emis.dhis2nigeria.org.ng/dhis/api in app.py.
HEADERS: Configured with the API token from the .env file.
DATASET_UIDS: Defined in app.py for specific datasets to monitor.

Development

Modify app.py for backend logic changes.
Update index.html for UI adjustments using React and Tailwind CSS.
Add new dependencies to requirements.txt as needed.
Test changes locally before deployment.

Contributing

Fork the repository.
Create a feature branch (git checkout -b feature-name).
Commit changes (git commit -m "Add feature-name").
Push to the branch (git push origin feature-name).
Open a pull request.

License
This project is licensed under the MIT License - see the LICENSE file for details.
Acknowledgments

Built with Flask, React, and Tailwind CSS.
Thanks to the DHIS2 community for the API and documentation.
