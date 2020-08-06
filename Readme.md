
## Overview

### Batch Load
1. In OpenMRS: loop over all relevant patients & their data 
2. Create a bundle for each patient
3. Send bundle to HAPI server
    - if exists: update
    - if does not exist: create
