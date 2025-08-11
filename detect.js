const fs = require("fs");
const path = require("path");

const folderPath = "./"; // change to your folder path

// Read all files in the folder
const files = fs.readdirSync(folderPath);

// Object to group by base name
const groups = {};

files.forEach(file => {
    // Remove extension if exists
    const fileName = path.parse(file).name;

    // Remove last underscore part
    const parts = fileName.split("_");
    if (parts.length > 1) {
        parts.pop(); // remove last part
    }
    const baseName = parts.join("_");

    // Group files
    if (!groups[baseName]) {
        groups[baseName] = [];
    }
    groups[baseName].push(file);
});

// Show groups that have duplicates
Object.keys(groups).forEach(base => {
    if (groups[base].length > 1) {
        console.log(`Duplicate base name: ${base}`);
        console.log("Files:", groups[base]);
        console.log("-----------");
    }
});
