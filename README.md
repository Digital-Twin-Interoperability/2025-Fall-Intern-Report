# MoonWalker-Collision
This repository contains the documentation, weekly meetings for Fall 2025 Internships, and required scripts to build and test collision and interaction between two rovers in a digital twin environment (Unity and Omniverse Isaac Sim)
**Note: NVIDIA Omniverse Isaac Sim has its setup documentation in [Omniverse Setup](https://github.com/Digital-Twin-Interoperability/2025-Fall-Intern-Report/blob/main/Scripts/Omniverse/Omniverse_Documentation.pdf)**

 # Project Goals
 - Rebuild previos Cohort's work of Digital Twin Interoperability with HSML schema
 - Allow collision and interaction between two rovers simulated in two different platforms.
 - Facilitate realistic physics between two simulation platforms.

 # Requirements
 ## NVIDIA Omniverse
 1. Computer specifications outlined by NVIDIA Omniverse: [Omniverse Requirements](https://docs.omniverse.nvidia.com/dev-guide/latest/common/technical-requirements.html)
 2. Python 3.12+
 3. Python libraries: omniverse_kit, requests, pxr, charset_normalizer, idna, urllib, usd core
## Unity
 1. Computer specifications outlined by Unity: [Unity Requirements](https://docs.unity3d.com/2022.2/Documentation/Manual/system-requirements.html)
 2. C#
 3. VSCode

## HSML API
1. Docker — Used to run the HSML API’s internal services.
2. MySQL (internal to HSML API) — Used by the HSML API for validation/verification data storage (not accessed directly by the simulations).
3. Kafka (internal to HSML API) — Used by the HSML API for validation/verification message streaming (producer/consumer).

# Required Scripts
## NVIDIA Omniverse
1. CadreAOmniAPIJSON.py: requests to the HSML API and sends data about Cadre from Omniverse
2. CadreAOmni_ConsumerAPIJSON.py: requests to the HSML and reads in data about Cadre from Unity for Unity hosting
3. ViperAOmniAPIJSON.py: requests to the HSML API and reads in data about Cadre from Unity for Unity hosting
4. rover_generalizer_final.py: takes in a rover prim and generalizes it for Omniverse hosting
5. collision_HSML.py: handles collision detection and hosting switch for rover.
## Unity
1. Viper: Asset with Mesh, Collider, & Rigidbody functionality.
2. Cadre: Asset with Mesh. Collider and Rigidbody created on collision only.
3. Lunar Surface: Asset for DT Lunar Surface Exploration.

## HSML API
1. hsml_api: Runs validation/verification and acts as the main endpoint both simulation platforms connect to. It receives real-time rover movement/physics data and routes it through the API’s internal services (including Kafka) for processing.
2. HSML-Form-Website: Used to register Spatial Web entities (e.g., rovers, agents, users, organizations) so they can be recognized and managed by the HSML system.

# Required USD/FBX files
## NVIDIA Omniverse
1. Viper Substance File (1).fbx
2. 4WD_Contrl.usd
3. CADREDEMO.usd
4. MAINDEMO.usd
5. Shackleton Ridge 300x300 v4 -001.usd
## Unity

# Usage
## NVIDIA Omniverse
1. Open up NVIDIA Omniverse
2. Run rover_generalizier.py to generalize the rover stage tree structure
3. Open 3 command prompts and run the following scripts as such in your python virtual environment:
  ```
  python CadreAOmniAPIJSON.py
  python CadreAOmni_ConsumerAPIJSON.py
  python ViperAOmniAPIJSON.py
  ```
5. Run the scripts above and press play on NVIDIA Omniverse
## Unity
## HSML API
1. Make sure to edit env. files to correct configuration of MySQL, and Kafka
2. Open Docker Desktop
3. Open 2 Ubuntu Terminals to run the hsml_api and HSML-Form-Website:
4. In **Terminal 1**, go to the `hsml_api` directory and start the internal services:
   ```bash
   cd hsml_api
   docker-compose up
5. In **Terminal 2**, go to the `HSML-Form-Website` directory and start the from app via Docker:
   ```bash
   cd HSML-Form-Website

   docker build -t hsml-form .

   docker run -p 5000:5000 hsml-form

 6. Now Docker Containers are in Docker so they can be run from inside Docker next time
