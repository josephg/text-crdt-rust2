use smallvec::SmallVec;
use skiplist::*;
use inlinable_string::InlinableString;
// use std::ptr;
// use std::iter;

pub type ClientName = InlinableString;
pub type ClientID = u16;
pub type ClientSeq = u32;


// More common/correct to use usize here but this will be fine in practice and faster.
pub type CharCount = u32;

pub const CLIENT_INVALID: ClientID = ClientID::MAX;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct CRDTLocation {
    pub client: ClientID,
    pub seq: ClientSeq,
}

impl Default for CRDTLocation {
    fn default() -> Self {
        CRDTLocation {
            client: CLIENT_INVALID,
            seq: 0
        }
    }
}

pub const CRDT_DOC_ROOT: CRDTLocation = CRDTLocation {
    client: CLIENT_INVALID,
    seq: 0
};

// These are always inserts. Deletes are not put into the skiplist.
#[derive(Copy, Clone, Debug)]
pub struct ListItem {
    loc: CRDTLocation,
    len: u32,

    /// I'm not sure what would be faster here. We could either make a fixed
    /// inline buffer for characters and split long inserts across multiple
    /// ListItem nodes, or handle long inserts by putting the characters in the
    /// heap with InlinableString.
    ///
    /// This is simpler for now so its what I'm going with, but it'd be
    /// interesting to benchmark and compare.
    content: u32,
    // content: InlinableString,

    // TODO.
    parent: (),


    
    // RGA
    // children: SmallVec<[CRDTLocation; 2]>,

}


impl skiplist::ListItem for ListItem {
    fn get_usersize(&self) -> usize { self.len as usize }

    fn split_item(&self, at: usize) -> (Self, Self) {
        println!("Split!");
        let at_u32 = at as u32;
        assert!(at_u32 < self.len);
        (ListItem {
            loc: self.loc, len: at_u32, content: at_u32, parent: ()
        }, ListItem {
            loc: CRDTLocation {
                client: self.loc.client,
                seq: self.loc.seq + at_u32,
            }, len: self.len - at_u32, content: self.len - at_u32, parent: ()
        })
    }
}

pub enum OpAction {
    // Insert(InlinableString),
    Insert(u32),
    // Deleted characters in sequence. In a CRDT these characters must be
    // contiguous from a single client.
    Delete(u32)
}

type Marker = ItemMarker<ListItem>;


// #[derive(Debug)]
struct ClientData {
    // Used to map from client's name / hash to its numerical ID.
    name: ClientName,

    // We need to be able to map each location to an item in the associated BST.
    // Note for inserts which insert a lot of contiguous characters, this will
    // contain a lot of repeated pointers. I'm trading off memory for simplicity
    // here, for now. This should be replaced with a 2-level tree or data
    // structure.
    ops: Vec<Marker>
}

type HistoricalData = Vec<ClientData>;

// #[derive(Debug)]
pub struct CRDTState {
    client_data: HistoricalData,

    document_index: SkipList<ListItem, HistoricalData>
    // document_index: Pin<Box<MarkerTree>>,

    // ops_from_client: Vec<Vec<
}

impl NotifyTarget<ListItem> for HistoricalData {
    fn notify(&mut self, items: &[ListItem], marker: ItemMarker<ListItem>) {
        for item in items {
            let loc = item.loc;
            let ops = &mut self[loc.client as usize].ops;
            for op in &mut ops[loc.seq as usize..(loc.seq+item.len) as usize] {
                *op = marker;
            }
        }
    }
}

// fn notify(items: &[ListItem], marker: Marker) {
//     // println!("Notify! {:?}", items);
// }


impl CRDTState {
    pub fn new() -> Self {
        CRDTState {
            client_data: Vec::new(),
            document_index: SkipList::new()
        }
    }

    pub fn get_or_create_clientid(&mut self, name: &str) -> ClientID {
        if let Some(id) = self.get_clientid(name) {
            id
        } else {
            // Create a new id.
            self.client_data.push(ClientData {
                name: InlinableString::from(name),
                ops: Vec::new()
            });
            (self.client_data.len() - 1) as ClientID
        }
    }

    fn get_clientid(&self, name: &str) -> Option<ClientID> {
        self.client_data.iter()
        .position(|client_data| &client_data.name == name)
        .map(|id| id as ClientID)
    }

    pub fn local_insert(&mut self, client_id: ClientID, pos: usize, inserted_length: usize) -> CRDTLocation {
        // First lookup and insert into the marker tree
        let ops = &mut self.client_data[client_id as usize].ops;
        let new_item = ListItem {
            loc: CRDTLocation {
                client: client_id,
                seq: ops.len() as ClientSeq
            },
            len: inserted_length as u32,
            content: inserted_length as u32,
            parent: ()
        };

        ops.resize(ops.len() + inserted_length, Marker::null());

        // Needed to work around the borrow checker.
        let client_data = &mut self.client_data;

        let (mut edit, offset) = self.document_index.edit_n(client_data, pos);

        let prev_item = if offset == 0 { edit.prev_item() }
        else { edit.current_item() };

        let (parent, needs_insert) = match prev_item {
            None => (CRDT_DOC_ROOT, true),
            Some(prev) => {
                // Try and append to the end of the item.
                // println!("prev {:?} offset {:?}", prev, offset);
                let parent = CRDTLocation {
                    client: prev.loc.client,
                    seq: prev.loc.seq + offset as u32
                };
                // if false {
                if offset == 0
                    && prev.loc.client == client_id
                    && prev.loc.seq + prev.len == new_item.loc.seq {
                    // Modify in place
                    edit.modify_prev_item(|item| {
                        item.len += inserted_length as u32;
                        item.content += inserted_length as u32;
                    });

                    (parent, false)
                } else { (parent, true) }
            }
        };

        if needs_insert { edit.insert(new_item); }

        parent
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut state = CRDTState::new();

        let fred = state.get_or_create_clientid("fred");

        state.local_insert(fred, 0, 1);
        state.local_insert(fred, 1, 4);

        println!("list {:?}", state.document_index);
    }


    #[test]
    fn append_end() {
        let mut state = CRDTState::new();
        let id = state.get_or_create_clientid("fred");

        let mut pos = 0;
        for _ in 0..1000 {
            state.local_insert(id, pos, 4);
            pos += 4;
        }

        assert_eq!(state.document_index.len_items(), 1);
    }

    #[test]
    fn randomizer() {
        let mut state = CRDTState::new();

        let fred = state.get_or_create_clientid("fred");
        let george = state.get_or_create_clientid("george");

        let mut pos = 0;
        for _i in 0..1000 {
            state.local_insert(fred, pos, 4);
            state.local_insert(george, pos + 4, 6);
            // state.insert_name("fred", pos, InlinableString::from("fred"));
            // state.insert_name("george", pos + 4, InlinableString::from("george"));
            pos += 10;
        }
    }
}
