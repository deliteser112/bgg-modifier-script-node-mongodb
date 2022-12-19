const MongoClient = require('mongodb').MongoClient

// initialize connection variables
const hostname = process.env.MONGO_HOSTNAME || '127.0.0.1'
const portnumber = process.env.MONGO_PORT || '27017'
const database = process.env.MONGO_DATABASE_NAME || 'meteor'
const tablename = 'Games'

async function main() {
  const connectionURL = `mongodb://${hostname}:${portnumber}`;
  const client = new MongoClient(connectionURL)
  try {
    // Connect to the MongoDB server
    await client.connect()

    // Get a reference to the "Games" collection
    const collection = client.db(`${database}`).collection(`${tablename}`)

    // Set the batch size to 2,000 games
    const BATCHSIZE = 2000

    // Create an array to hold the update operations
    const updates = []

    // Get total games from database
    const totalGames = await collection.count({})
    const batchCounts = Math.ceil(totalGames / BATCHSIZE)

    // Set the initial modified date
    let modified = new Date()

    // Process the games in batches
    let batchNumber = 0
    while (batchCounts > batchNumber) {
      // Output a message
      console.log(
        `Processed a batch of modified dates for ${modified} ---- ${
          batchNumber * BATCHSIZE
        } ~ ${(batchNumber + 1) * BATCHSIZE}`,
      )

      // Get the next batch of games
      const cursor = collection.find(
        {},
        { skip: batchNumber * BATCHSIZE, limit: BATCHSIZE },
      )
      const batch = await cursor.toArray()

      // Add an update operation for each game in the batch
      batch.forEach((doc) => {
        updates.push({
          updateMany: {
            filter: { _id: doc._id },
            update: { $set: { modified: modified } },
          },
        })
      })

      // If the updates array has reached the batch size, execute the operations
      if (updates.length === BATCHSIZE) {
        await collection.bulkWrite(updates)
        updates.length = 0
      }

      // Add one day to the modified date
      modified = new Date(modified.getTime() + 86400000)

      // Increase skip number for batch
      batchNumber++
    }

    // Execute any remaining update operations
    if (updates.length > 0) {
      await collection.bulkWrite(updates)
    }

    console.log('Games updated successfully')
  } catch (e) {
    console.error(e)
  } finally {
    // Close the connection to the MongoDB server
    await client.close()
  }
}

main()
