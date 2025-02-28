const { MongoClient, ObjectId } = require("mongodb");
const uri = require("./atlas_uri");

const client = new MongoClient(uri);
const dbName = "Bookmark_ManagerDB";
const Bookmark_collection = "Bookmarks";
const user_collection = "User";
const bookMarkCollection = client.db(dbName).collection(Bookmark_collection);
const userCollection = client.db(dbName).collection(user_collection);

//User
const insertManyUserRecord = async (user) => {
  const result = await userCollection.insertMany(user);
  console.log(`User document inserted successfully`, result.insertedCount);
};

const insertSingleUserRecord = async (user) => {
  const result = await userCollection.insertOne(user);
  console.log(`User document inserted successfully`, result.insertedCount);
};

const getUserRecord = async (id) => {
  const user = await userCollection.findOne({ _id: ObjectId(id) });
  console.log("User:", user);
};
const getAllUserRecord = async () => {
  const user = await userCollection.find({}).toArray();
  console.log("User:", user);
};

// Bookmark
const insertSingleBookmarkRecord = async (bookmark) => {
  const result = await bookMarkCollection.insertOne(bookmark);
  console.log(`Inserted document with ID: ${result.insertedCount}`);
};

const insertManyBookmarkRecord = async (bookmark) => {
  const result = await bookMarkCollection.insertMany(bookmark);
  console.log(`Inserted document with ID: ${result.insertedCount}`);
};

const updateBookmarkRecord = async (id, update) => {
  const result = await bookMarkCollection.updateOne(
    { _id: new ObjectId(id) },
    { $set: update }
  );
  console.log("Bookmark updated:", result.modifiedCount);
};

const deleteBookmarkRecord = async (id) => {
  const result = await bookMarkCollection.deleteOne({ _id: new ObjectId(id) });
  if (result.deletedCount === 1) {
    console.log("Bookmark deleted successfully");
  } else {
    console.log("No document found to delete");
  }
};

const getSingleBookmarkRecord = async (id) => {
  const bookmark = await bookMarkCollection.findOne({ _id: new ObjectId(id) });
  console.log("Bookmark:", bookmark);
};

const getAllBookmarkRecord = async () => {
  const bookmarks = await bookMarkCollection.find().toArray();
  console.log("Bookmarks:", bookmarks);
};

// Tag based search
const tagBasedSearch = async (tag) => {
  const bookmarks = await bookMarkCollection.find({ tags: tag }).toArray();
  console.log(`Bookmarks tagged with "${tag}":`, bookmarks);
  //   const bookmarks = await bookMarkCollection
  //     .find({ tags: tag })
  //     .explain("executionStats");
};

//Full-text search
const fullTextSearch = async (search) => {
  const result = await bookMarkCollection
    .find({ $text: { $search: search } })
    .toArray();
  console.log("Search results:", result);
};

// pipeline
const pipelineSearch = async (search, tag) => {
  const pipeline = [
    {
      $match: { $and: [{ $text: { $search: search } }, { tags: tag }] },
    },
    {
      $sort: { createdAt: -1 },
    },
    {
      $limit: 2,
    },
  ];

  const result = await bookMarkCollection.aggregate(pipeline).toArray();
  console.log("Pipeline search results:", result);
};

//fetch bookmark with user details
const fetchBookmarkWithUserDetails = async () => {
  const result = await bookMarkCollection
    .aggregate([
      {
        $lookup: {
          from: "User",
          localField: "userId",
          foreignField: "_id",
          as: "userDetails",
        },
      },
      { $unwind: "$userDetails" },
      {
        $project: {
          _id: 1,
          title: 1,
          url: 1,
          description: 1,
          tags: 1,
          isPublic: 1,
          createdAt: 1,
          "userDetails._id": 1,
          "userDetails.name": 1,
          "userDetails.email": 1,
        },
      },
    ])
    .toArray();
  console.log(result);
};

// most used public tags
const mostUsedTags = async () => {
  const result = await bookMarkCollection.aggregate([
    { $match: { isPublic: true } },
    { $group: { _id: "$tags", count: { $sum: 1 } } },
    { $sort: { count: -1 } },
    { $limit: 1 },
  ]);
};

const main = async () => {
  try {
    await client.connect();
    console.log("Connected to the database");
    await getAllBookmarkRecord();
    await fullTextSearch("GUIDE");
    await pipelineSearch("guide", "database");
    await getAllUserRecord();
    await fetchBookmarkWithUserDetails();
  } catch (error) {
    console.log("main ", error);
  } finally {
    await client.close();
  }
};

main();
