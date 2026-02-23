// ==================================================
// MongoDB: Create Read-Only User
// ==================================================
// Run this script:
// mongosh -u admin -p admin_4321! --authenticationDatabase admin < create_readonly_users.js

// Connect to your database
db = db.getSiblingDB('dopamas_old_data_db');

// Create read-only user
db.createUser({
  user: 'readonly_user',
  pwd: 'changeme_readonly_pass_123',
  roles: [
    {
      role: 'read',
      db: 'dopamas_old_data_db'
    }
  ]
});

print('========================================');
print('MongoDB Read-Only User Created!');
print('User: readonly_user');
print('Password: changeme_readonly_pass_123');
print('Database: dopamas_old_data_db');
print('');
print('IMPORTANT: Change the password!');
print('IMPORTANT: Update .env file with this password!');
print('========================================');

// Display collections
print('\nAvailable collections:');
db.getCollectionNames().forEach(function(collection) {
    print('  - ' + collection);
});

// Verify user
print('\nVerifying user creation:');
db.getUsers().forEach(function(user) {
    if (user.user === 'readonly_user') {
        print('✓ User exists with roles:');
        user.roles.forEach(function(role) {
            print('  - ' + role.role + ' on ' + role.db);
        });
    }
});

