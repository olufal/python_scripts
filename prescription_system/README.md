We've built a platform that powers our pharmacy. One of its services is the Actions system: 
   - responding to a patient's message - 30 mins from created datetime
   - requesting a renewal from a doctor - 
   - conducting a clinical assessment -
   - troubleshooting an insurance rejection - 3 hours from create datetime
   
Each action has a time limit. For example, we want to complete the "respond to patient message" action within 30 minutes 
of it being created and complete "troubleshoot insurance rejection" action within 3 hours of it being created. 
We want an employee to click a button and pull the next most important outstanding action for them to work on using the 
following rules: 
- An employee should only be able to pull one action at a time 
- An employee should not be able to pull an action that was already pulled by a different employee 
- An employee should only receive actions that match their skills 
- An employee should pull actions that are closest to missing their time limit before pulling other actions with more time 

0. Design the minimum necessary data structures to support this system. 
0. Write the code for a pull_next_action method that accepts an employee object and returns an action object for the 
next most important action for that employee.