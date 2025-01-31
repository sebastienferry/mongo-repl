
use("Animals");
db.Items.insertMany([
    { _id: ObjectId('000000000000000000000001'), name: "one" },
    { _id: ObjectId('000000000000000000000002'), name: "two" },
    { _id: ObjectId('000000000000000000000003'), name: "three" },
    { _id: ObjectId('000000000000000000000004'), name: "four" },
]);


// use("Animals");
// db.Vets.insertOne({ name: "Dr. Dolittle", specialty: "Exotic Animals" });

// // Insert a list of 10 veterinarians
// use("Animals");
// db.Vets.insertMany([
//     { name: "Dr. Dolittle", specialty: "Exotic Animals" },
//     { name: "Dr. Pol", specialty: "Farm Animals" },
//     { name: "Dr. Jeff", specialty: "Large Animals" },
//     { name: "Dr. K", specialty: "Small Animals" },
//     { name: "Dr. Oakley", specialty: "Large Animals" },
//     { name: "Dr. Ross", specialty: "Farm Animals" },
//     { name: "Dr. Blue", specialty: "Small Animals" },
//     { name: "Dr. Emily", specialty: "Exotic Animals" },
//     { name: "Dr. Brenda", specialty: "Small Animals" },
//     { name: "Dr. Michelle", specialty: "Large Animals" }
// ]);

// // Create an index on the specialty field
// use("Animals");
// db.Vets.createIndex({ specialty: 1 });

// // Create an index on the specialty field
// use("Animals");
// db.Vets.dropIndex("specialty_1");

// // Insert a weekly schedule for each veterinarian
// db.Vets.find().forEach(function(vet) {
//     db.Schedules.insertOne({
//         vetId: vet._id,
//         days: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
//         hours: ["8:00am", "9:00am", "10:00am", "11:00am", "1:00pm", "2:00pm", "3:00pm", "4:00pm"]
//     });
// });

// // Insert a list of 10 animals
// use("Animals");
// db.Pets.insertMany([
//     { name: "Fido", species: "Dog", breed: "Golden Retriever", age: 3, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Whiskers", species: "Cat", breed: "Siamese", age: 5, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Bubbles", species: "Fish", breed: "Goldfish", age: 1, vetId: db.Vets.findOne({ specialty: "Exotic Animals" })._id },
//     { name: "Fluffy", species: "Rabbit", breed: "Holland Lop", age: 2, vetId: db.Vets.findOne({ specialty: "Exotic Animals" })._id },
//     { name: "Spot", species: "Cow", breed: "Holstein", age: 4, vetId: db.Vets.findOne({ specialty: "Farm Animals" })._id },
//     { name: "Polly", species: "Parrot", breed: "African Grey", age: 6, vetId: db.Vets.findOne({ specialty: "Exotic Animals" })._id },
//     { name: "Mittens", species: "Cat", breed: "Maine Coon", age: 7, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Rex", species: "Dog", breed: "German Shepherd", age: 5, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Bella", species: "Dog", breed: "Labrador Retriever", age: 2, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Charlie", species: "Cat", breed: "Persian", age: 3, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Max", species: "Dog", breed: "Bulldog", age: 4, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Buddy", species: "Dog", breed: "Beagle", age: 6, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Oscar", species: "Cat", breed: "Bengal", age: 2, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Milo", species: "Dog", breed: "Poodle", age: 1, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Daisy", species: "Dog", breed: "Shih Tzu", age: 3, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Luna", species: "Cat", breed: "Sphynx", age: 4, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Lucy", species: "Dog", breed: "Chihuahua", age: 5, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Rocky", species: "Dog", breed: "Boxer", age: 6, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Simba", species: "Cat", breed: "Ragdoll", age: 2, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Coco", species: "Dog", breed: "Dachshund", age: 3, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Bailey", species: "Dog", breed: "Cocker Spaniel", age: 4, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Nala", species: "Cat", breed: "British Shorthair", age: 5, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Chloe", species: "Dog", breed: "Pug", age: 6, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Zoe", species: "Cat", breed: "Abyssinian", age: 2, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Jack", species: "Dog", breed: "Border Collie", age: 3, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Lily", species: "Dog", breed: "Yorkshire Terrier", age: 4, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Toby", species: "Dog", breed: "Siberian Husky", age: 5, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Molly", species: "Cat", breed: "Scottish Fold", age: 6, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Duke", species: "Dog", breed: "Great Dane", age: 2, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Sadie", species: "Dog", breed: "Rottweiler", age: 3, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Ruby", species: "Cat", breed: "Siberian", age: 4, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Ginger", species: "Dog", breed: "Cavalier King Charles Spaniel", age: 5, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Pepper", species: "Cat", breed: "American Shorthair", age: 6, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Riley", species: "Dog", breed: "Australian Shepherd", age: 2, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Sophie", species: "Dog", breed: "Bichon Frise", age: 3, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Penny", species: "Cat", breed: "Birman", age: 4, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Harley", species: "Dog", breed: "Mastiff", age: 5, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Buster", species: "Dog", breed: "Saint Bernard", age: 6, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Maggie", species: "Cat", breed: "Oriental Shorthair", age: 2, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Rocco", species: "Dog", breed: "Doberman Pinscher", age: 3, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Loki", species: "Dog", breed: "Akita", age: 4, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Oliver", species: "Cat", breed: "Russian Blue", age: 5, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Bentley", species: "Dog", breed: "Bernese Mountain Dog", age: 6, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Hazel", species: "Dog", breed: "Weimaraner", age: 2, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Gizmo", species: "Cat", breed: "Devon Rex", age: 3, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Bruno", species: "Dog", breed: "Newfoundland", age: 4, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Jasper", species: "Dog", breed: "Samoyed", age: 5, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Willow", species: "Cat", breed: "Turkish Angora", age: 6, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id },
//     { name: "Ace", species: "Dog", breed: "Alaskan Malamute", age: 2, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Finn", species: "Dog", breed: "Whippet", age: 3, vetId: db.Vets.findOne({ specialty: "Large Animals" })._id },
//     { name: "Shadow", species: "Cat", breed: "Norwegian Forest Cat", age: 4, vetId: db.Vets.findOne({ specialty: "Small Animals" })._id }
// ]);
