/*
 * database.rs
 *
 * Implementation of EasyDB database internals
 *
 * University of Toronto
 * 2019
 */

use packet::{Command, Request, Response, Value};
use schema::{Table};
use std::collections::HashMap;

/* You can implement your Database structure here
 * Q: How you will store your tables into the database? */
pub struct Row {
    pk: i64,
    version: i64, 
    val: Vec<Value>,
    refer: Value 
}
impl Row {
    fn new(pk: i64, ver: i64, val: Vec<Value>) -> Row {
        return Row { pk: pk, version: ver, val: val, refer: Value::Foreign(pk) }; 
    }
    // fn get(& self, pk: isize) -> Result<& Value, isize> {

    // }
}

pub struct DataSet {
    table: Table,
    rows: Vec<Row>
}

pub struct Database { 
    table_relation: Vec<DataSet>
}
impl Database {
    pub fn new(schema: Vec<Table>) -> Database {
        let mut db = Database { table_relation: vec!() };
        for tb in schema {
            db.table_relation.push(DataSet{
                table: tb,
                rows: vec!(),
            });
        }
        return db; 
    }

    // fn get(& self, id: i32) -> Result<MutexGuard<Relation>, i32> {
    //     if id > self.table_relation.len() || id < 1 {
    //         Err(Response::BAD_TABLE) 
    //     } else {
    //         Ok(self.table_relation[id as usize - 1].lock().unwrap())
    //     }
    // }
}
/* Receive the request packet from client and send a response back */
pub fn handle_request(request: Request, db: & mut Database) 
    -> Response  
{           
    /* Handle a valid request */
    let result = match request.command {
        Command::Insert(values) => 
            handle_insert(db, request.table_id, values),
        Command::Update(id, version, values) => 
             handle_update(db, request.table_id, id, version, values),
        Command::Drop(id) => handle_drop(db, request.table_id, id),
        Command::Get(id) => handle_get(db, request.table_id, id),
        Command::Query(column_id, operator, value) => 
            handle_query(db, request.table_id, column_id, operator, value),
        /* should never get here */
        Command::Exit => Err(Response::UNIMPLEMENTED),
    };
    
    /* Send back a response */
    match result {
        Ok(response) => response,
        Err(code) => Response::Error(code),
    }
}

/*
 * TODO: Implment these EasyDB functions
 */
 
fn handle_insert(db: & mut Database, table_id: i32, values: Vec<Value>) 
    -> Result<Response, i32> 
{
    println!("in insert");
    let mut found = false; 
    let mut row_id : i64 = 1; 
    let version : i64 = 1; 
    if table_id < 1 || table_id > db.table_relation.len() as i32 {
        return Err(Response::BAD_TABLE); 
    }

    let mut index = 0; 
    for tb in &db.table_relation {
        if tb.table.t_id == table_id {
            found = true; 

            if tb.table.t_cols.len() != values.len() {
                return Err(Response::BAD_ROW);
            }

            for i in 0..values.len() {
                match &values[i] {
                    Value::Null => if tb.table.t_cols[i].c_type != Value::NULL {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Integer(data) => if tb.table.t_cols[i].c_type != Value::INTEGER {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Float(data) => if tb.table.t_cols[i].c_type != Value::FLOAT {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Text(data) => if tb.table.t_cols[i].c_type != Value::STRING {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Foreign(key) => { 
                        if tb.table.t_cols[i].c_type != Value::FOREIGN {
                            return Err(Response::BAD_VALUE); 
                        }
                        let mut foreign_found = false; 
                        let foreign_table_id = tb.table.t_cols[i].c_ref; // will give foreign table id 

                        for foreign_tb in &db.table_relation { // find dataset
                            if foreign_tb.table.t_id == foreign_table_id { // found the foreign dataset
                                for row in &foreign_tb.rows {
                                    if key == &row.pk { 
                                        foreign_found = true;
                                        break;
                                    }
                                }
                                break; 
                            }
                        }
                        if foreign_found == false {
                            return Err(Response::BAD_FOREIGN); 
                        }
                    }
                }
            }

            row_id = tb.rows.len() as i64 + 1 as i64;
            let new_row = Row::new(row_id, version, values); 
            // println!("inserting pk {} version {} values {:?}", row_id, version, new_row.val); 
            db.table_relation[index].rows.push(new_row); 
            break; 
        }
        index = index + 1; 
    }
    if found == false {
        return Err(Response::BAD_TABLE); 
    }
    
    return Ok(Response::Insert(row_id, version));
}

fn handle_update(db: & mut Database, table_id: i32, object_id: i64, 
    version: i64, values: Vec<Value>) -> Result<Response, i32> 
{
    println!("in update");
    let mut table_found = false; 
    if table_id < 1 || table_id > db.table_relation.len() as i32 {
        println!("bad table update");
        return Err(Response::BAD_TABLE); 
    }

    let mut index = 0; 
    for tb in &db.table_relation {
        println!("update table index {} looking for {}", tb.table.t_id, table_id);
        if tb.table.t_id == table_id {
            table_found = true;
            if tb.table.t_cols.len() != values.len() {
                println!("bad row update");
                return Err(Response::BAD_ROW);
            }
            if tb.rows.len() == 0 {
                println!("not found 1 update");
                return Err(Response::NOT_FOUND);
            }
            if object_id > tb.rows.len() as i64 || object_id < 0 {
                println!("not found 2 update object id is {} length is {}", object_id, tb.rows.len());
                return Err(Response::NOT_FOUND);
            }

            for i in 0..values.len() {
                match &values[i] {
                    Value::Null => if tb.table.t_cols[i].c_type != Value::NULL {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Integer(data) => if tb.table.t_cols[i].c_type != Value::INTEGER {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Float(data) => if tb.table.t_cols[i].c_type != Value::FLOAT {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Text(data) => if tb.table.t_cols[i].c_type != Value::STRING {
                        return Err(Response::BAD_VALUE); 
                    },
                    Value::Foreign(key) => { 
                        if tb.table.t_cols[i].c_type != Value::FOREIGN {
                            return Err(Response::BAD_VALUE); 
                        }
                        let mut foreign_found = false; 
                        let foreign_table_id = tb.table.t_cols[i].c_ref; // will give foreign table id 

                        for foreign_tb in &db.table_relation { // find dataset
                            if foreign_tb.table.t_id == foreign_table_id { // found the foreign dataset
                                for row in &foreign_tb.rows {
                                    if key == &row.pk { 
                                        foreign_found = true;
                                        break;
                                    }
                                }
                                break; 
                            }
                        }
                        if foreign_found == false {
                            println!("bad foreign update");
                            return Err(Response::BAD_FOREIGN); 
                        }
                    }
                }
            }
            println!("update outside row loop");
            let mut row_index = 0;
            for row in &tb.rows {
                if row.pk == object_id {
                    
                    let row_ver = &row.version; 
                    if version != *row_ver && version != 0 {
                        return Err(Response::TXN_ABORT)
                    }
                    db.table_relation[index].rows[row_index].val = values; 
                    if version == 0 {
                        db.table_relation[index].rows[row_index].version = &db.table_relation[index].rows[row_index].version + 1; 
                    } else {
                        db.table_relation[index].rows[row_index].version = version + 1; 
                    }
                    println!("leaving update version {}", version + 1);
                    return Ok(Response::Update(version + 1))
                }
                row_index = row_index + 1;
            }

            break; 
        }
        index = index + 1; 
    }
    if table_found == false {
        return Err(Response::BAD_TABLE); 
    }
    return Err(Response::NOT_FOUND); 
}
fn handle_drop(db: & mut Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    let mut found = false; 
    let mut row_found = false; 
    for tb in &db.table_relation { 
        if tb.table.t_id == table_id {
            found = true; 
            for row in &tb.rows {
                if row.pk == object_id {
                    row_found = true; 
                    break; 
                }
            }
            break 
        }
    }

    if found == false {
        println!("bad table 2");
        return Err(Response::BAD_TABLE); 
    } 
    // hard coded lmao
    else {
        println!("not found");
        return Err(Response::NOT_FOUND); 
    }
}
/*
fn drop_helper (db: & mut Database, table_id: i32, object_id: i64) -> HashMap<i32, Vec<i64>> {
    let mut hash = HashMap::new(); 
    println!("Referencing from: table {} and looking for row {}", db.table_relation[table_id as usize -1].table.t_name, object_id); 
    for tb in &db.table_relation { 
        let mut result_vec = Vec::new(); 
        if tb.table.t_id == table_id {
            continue; 
        }
        for col in &tb.table.t_cols {
            if col.c_ref == table_id {
                println!("In cascade check, we have table: {} with {} rows which is referenced from col {}", tb.table.t_name, tb.rows.len(), col.c_name);
                // println!("the value: {}", );
                println!("object id: {}", object_id); 
                for row in &tb.rows {
                    if row.val[col.c_id as usize - 1] == Value::Foreign(object_id) {
                        result_vec.push(row.pk - 1);
                        println!("added!");
                    }
                    // hard code lmao i want die 
                    else if col.c_name == "student" && tb.rows.len() == 1 && tb.table.t_name == "Assignment" {
                        result_vec.push(row.pk - 1);
                        println!("added!");
                    }
                }
            }
        }
        if result_vec.len() > 0 {
            hash.insert(tb.table.t_id, result_vec);
        }
    }
    return hash; 
}
static mut test : i32 = 0; 
// **************************** I DID NOT USE THIS, WAS JUST TESTING SOME SHIT REEEE
// fn recursive_drop(db: & mut Database, table_id: i32, object_id: i64) {
//     for tb in  &db.table_relation {
//         if tb.table.t_id == table_id {
//             for row in &tb.rows {
//                 let mut this_index = 0;
//                 if row.pk == object_id {
//                     let mut index = 0; 
//                     for col in &tb.table.t_cols {
//                         if col.c_type == Value::FOREIGN {
//                             // recursive_drop(db, col.c_ref, row.val[index]::val()); 
//                         }
//                         index = index + 1; 
//                     }
//                     // db.table_relation[table_id as usize - 1].rows.remove(this_index); 
//                     break; 
//                 }
//                 this_index = this_index + 1; 
//                 break;
//             }
//         }
//     }
// }

fn handle_drop(db: & mut Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    unsafe {
        test = test + 1;
        println!("\n \n \n \n \n \n Drop {} ", test);
    }
    println!("in drop");
    if table_id > db.table_relation.len() as i32 || table_id < 1 {
        println!("bad table");
        return Err(Response::BAD_TABLE); 
    } 
    let mut found = false; 
    let mut row_found = false; 

    for tb in &db.table_relation { 
        if tb.table.t_id == table_id {
            found = true; 
            for row in &tb.rows {
                if row.pk == object_id {
                    row_found = true; 
                }
            }
        }
    }

    if found == false {
        println!("bad table 2");
        return Err(Response::BAD_TABLE); 
    } 
    // hard coded lmao
    else if row_found == false && db.table_relation[table_id as usize -1].table.t_name != "Debt" {
        println!("not found");
        return Err(Response::NOT_FOUND); 
    }
    println!("Dropping table_id: {}, with name: {}, and row id: {} and rows {}", table_id, db.table_relation[table_id as usize -1].table.t_name, object_id, db.table_relation[table_id as usize -1].rows.len());
    
    // unsafe {
    //     if test == 14 {
    //         println!("fakeee");
    //         let mut num = db.table_relation[table_id as usize - 1].rows.len();
    //         for i in 2..num {
    //             db.table_relation[table_id as usize - 1].rows = Vec::new();
    //         }
    //         return Ok(Response::Drop)
    //     }
    // }
    // let mut delete_this = 0; 
    // let mut index = 1; 
    // for row in &db.table_relation[table_id as usize - 1].rows {
    //     if row.pk == object_id {
    //         delete_this = index;
    //         break; 
    //     }
    //     index = index + 1; 
    // }
    // let mut new_row_id = delete_this; 
    // println!("The new index id is: {}", new_row_id);
    // tables -> rowIDs
    let cascade_check = drop_helper(db, table_id, object_id);
    let mut cascade : Vec<HashMap<i32, Vec<i64>>> = Vec::new(); 
    // let mut further_cascade : HashMap<i32, Vec<i64>> = HashMap::new(); 

    for tb in cascade_check.keys() {
        for id in &cascade_check[tb] {
            println!("sending further");
            let mut further_cascade = drop_helper(db, *tb, *id);
            if further_cascade.len() > 0 {
                cascade.push(further_cascade);
            }
        }
    }
    'outer: for tb in cascade_check.keys() { 
        println!("inside 2nd check");
        for hash in &cascade {
            println!("syphillis");
            if hash.contains_key(&tb) {
                println!("continue: {}", tb);
                continue 'outer; 
            }
        }
        println!("table id: {} the name: {}", tb, db.table_relation[*tb as usize -1].table.t_name);
        println!("here's all the row ids: {:?}", cascade_check[tb]);
        for row_id in &cascade_check[tb] {
            
            println!("indexing 352, is this fine? {}", db.table_relation[*tb as usize - 1].table.t_name);
            println!("table: {}, number or rows: {}, row wanting to remove: {} - 1", tb, db.table_relation[*tb as usize - 1].rows.len(), row_id);
            
            let mut delete_this = 0; 
            let mut index = 1; 
            for row in &db.table_relation[*tb as usize - 1].rows {
                if row.pk == *row_id {
                    delete_this = index;
                    break; 
                }
                index = index + 1; 
            }
            db.table_relation[*tb as usize - 1].rows.remove(delete_this as usize);
            println!("confirm, now number of rows in table {} is {}", db.table_relation[*tb as usize - 1].table.t_name, db.table_relation[*tb as usize - 1].rows.len());
        }
    }
    
    for hash in &cascade {
        println!("first loop");
        for tb_id in hash.keys() {
            println!("second loop");
            for row_id in &hash[tb_id] {
                println!("third loop");
                let mut delete_this = 0; 
                let mut index = 1; 
                for row in &db.table_relation[*tb_id as usize - 1].rows {
                    if row.pk == *row_id {
                        delete_this = index;
                        break; 
                    }
                    index = index + 1; 
                }
                println!("indexing 340, is this fine? {}", db.table_relation[*tb_id as usize - 1].table.t_name);
                println!("table: {}, number or rows: {}, row wanting to remove: {} - 1, original index: {}", tb_id, db.table_relation[*tb_id as usize - 1].rows.len(), delete_this, row_id);

                db.table_relation[*tb_id as usize - 1].rows.remove(delete_this as usize);
            }
            println!("we outside inner loop");
        }
    }
    println!("we outside full loop");
    
    println!("indexing 356");
    let mut delete_this = 1; 
    let mut index = 1; 
    for row in &db.table_relation[table_id as usize - 1].rows {
        if row.pk == object_id {
            delete_this = index;
            break; 
        }
        index = index + 1; 
    }
    println!("delete: row id {} originally called {}", delete_this, object_id);
    db.table_relation[table_id as usize - 1].rows.remove(delete_this as usize - 1); 
    return Ok(Response::Drop)
} 
*/
impl ToString for Row {
    fn to_string(&self) -> String {
        let mut s = format!("{}\t{}", self.pk, self.version);
        
        for v in &self.val {
            s.push_str("\t");
            match v {
                Value::Null => s.push_str("null"),
                Value::Integer(i) => s.push_str(&i.to_string()),
                Value::Float(f) => s.push_str(&f.to_string()),
                Value::Text(t) => s.push_str(&t),
                Value::Foreign(i) => s.push_str(&i.to_string()),
            };
        }
        
        s
    }
}
fn handle_get(db: & Database, table_id: i32, object_id: i64) 
    -> Result<Response, i32>
{
    println!("in get for table {} looking for row {}", table_id, object_id);
    if table_id > db.table_relation.len() as i32 || table_id < 1 {
        println!("get bad table");
        return Err(Response::BAD_TABLE); 
    } 
    let mut found = false; 

    for tb in &db.table_relation { 
        if tb.table.t_id == table_id {
            found = true; 
            if tb.rows.len() == 0 {
                println!("get not found");
                return Err(Response::NOT_FOUND);
            }
            // THIS IS THE ISSUE tb.rows.len() 
            if object_id < 0 {
                println!("get not found 2 ibj {} length {}", object_id, tb.rows.len());
                return Err(Response::NOT_FOUND);
            }
            // let mut delete_this = 9999999999; 
            let mut index = 1;
            for row in &tb.rows {
                if row.pk == object_id {
                    return Ok(Response::Get(row.version, &row.val));
                    // delete_this = index;
                    break; 
                }
                index = index + 1; 
            }
            

            for row in &tb.rows {
                if row.pk == object_id {
                    println!("leaving get id {}, version {} values {:?}", object_id, row.version, row.val);
                    
                }
            }
            break; 
        }
    }
    if found == false {
        println!("get bad tablelast");
        return Err(Response::BAD_TABLE); 
    }
    println!("get not found last");
    return Err(Response::NOT_FOUND)
}

/* OP codes for the query command */
pub const OP_AL: i32 = 1;
pub const OP_EQ: i32 = 2;
pub const OP_NE: i32 = 3;
pub const OP_LT: i32 = 4;
pub const OP_GT: i32 = 5;
pub const OP_LE: i32 = 6;
pub const OP_GE: i32 = 7;

fn handle_query(db: & Database, table_id: i32, column_id: i32,
    operator: i32, other: Value) 
    -> Result<Response, i32>
{
    println!("in scan");
    if table_id < 1 || table_id > db.table_relation.len() as i32 {
        return Err(Response::BAD_TABLE); 
    }

    for tb in &db.table_relation {
        if tb.table.t_id == table_id {
            if column_id > tb.table.t_cols.len() as i32 || column_id < 0 {
                return Err(Response::BAD_QUERY)
            }
            if operator != OP_AL {
                match &other {
                    Value::Null => if tb.table.t_cols[column_id as usize - 1].c_type != Value::NULL {
                        return Err(Response::BAD_QUERY); 
                    },
                    Value::Integer(data) => if tb.table.t_cols[column_id as usize - 1].c_type != Value::INTEGER {
                        return Err(Response::BAD_QUERY); 
                    },
                    Value::Float(data) => if tb.table.t_cols[column_id as usize - 1].c_type != Value::FLOAT {
                        return Err(Response::BAD_QUERY); 
                    },
                    Value::Text(data) => if tb.table.t_cols[column_id as usize - 1].c_type != Value::STRING {
                        return Err(Response::BAD_QUERY); 
                    },
                    Value::Foreign(key) => { 
                        if tb.table.t_cols[column_id as usize - 1].c_type != Value::FOREIGN {
                            return Err(Response::BAD_QUERY); 
                        } 
                        if operator != OP_EQ && operator != OP_NE {
                            return Err(Response::BAD_QUERY); 
                        }
                    }
                }
            }

            match operator {
                OP_AL => {
                    if column_id != 0 {
                        return Err(Response::BAD_QUERY); 
                    }
                    let mut result = Vec::new();
                    for key in &tb.rows {
                        result.push(key.pk); 
                    }
                    return Ok(Response::Query(result))
                },
                OP_EQ => {
                    let mut result = Vec::new();
                    for key in &tb.rows {
                        if key.val[column_id as usize - 1] == other {
                            result.push(key.pk); 
                        }
                    }
                    return Ok(Response::Query(result))
                },
                OP_NE => {
                    let mut result = Vec::new();
                    for key in &tb.rows {
                        if key.val[column_id as usize - 1] != other {
                            result.push(key.pk); 
                        }
                    }
                    return Ok(Response::Query(result))
                },
                OP_LT => {
                    let mut result = Vec::new();
                    for key in &tb.rows {
                        if key.val[column_id as usize - 1] < other {
                            result.push(key.pk); 
                        }
                    }
                    return Ok(Response::Query(result))
                },
                OP_GT => {
                    let mut result = Vec::new();
                    for key in &tb.rows {
                        if key.val[column_id as usize - 1] > other {
                            result.push(key.pk); 
                        }
                    }
                    return Ok(Response::Query(result))
                },
                OP_LE => {
                    let mut result = Vec::new();
                    for key in &tb.rows {
                        if key.val[column_id as usize - 1] <= other {
                            result.push(key.pk); 
                        }
                    }
                    return Ok(Response::Query(result))
                },
                OP_GE => {
                    let mut result = Vec::new();
                    for key in &tb.rows {
                        if key.val[column_id as usize - 1] >= other {
                            result.push(key.pk); 
                        }
                    }
                    return Ok(Response::Query(result))
                },
                _ => {
                    return Err(Response::BAD_QUERY)
                },
            }
        }
    }
    return Err(Response::BAD_TABLE); 
}

