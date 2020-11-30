package ovskv

import (
	"fmt"
	"path"

	"github.com/ebay/libovsdb"
)

// Taken from etcd. Thanks.
// A key-value pair will have a string value
// A directory will have a children map
type node struct {
	Path string

	CreatedIndex  uint64
	ModifiedIndex uint64

	Parent *node `json:"-"` // should not encode this field! avoid circular dependency.

	Data       *libovsdb.ResultRow  // for key-value pairs
	Children   map[string]*node // for directory
}

// newKV creates a Key-Value pair
func newKV(nodePath string, data *libovsdb.ResultRow, createdIndex uint64, parent *node) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		Data:          data,
	}
}

// newDir creates a directory
func newDir(nodePath string, createdIndex uint64, parent *node) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		Children:      make(map[string]*node),
	}
}

func (n *node) UUID() string {
	return (*n.Data)["_uuid"].(libovsdb.UUID).GoUUID
}

func (n *node) Map() map[interface {}]interface {} {
	return (*n.Data)["data"].(libovsdb.OvsMap).GoMap
}

func (n *node) Value() string {
	return (*n.Data)["data"].(libovsdb.OvsMap).GoMap["v"].(string)
}

func (n *node) Key() string {
	if n.IsDir() {
		return n.Path
	}
	return pathKey((*n.Data)["path"])
}

// IsDir function checks whether the node is a directory.
// If the node is a directory, the function will return true.
// Otherwise the function will return false.
func (n *node) IsDir() bool {
	return n.Children != nil
}

// GetChild function returns the child node under the directory node.
// On success, it returns the file node
func (n *node) GetChild(name string) (*node, error) {
	if !n.IsDir() {
		return nil, fmt.Errorf("not a directory")
	}

	child, ok := n.Children[name]

	if ok {
		return child, nil
	}

	return nil, nil
}

// Add function adds a node to the receiver node.
// If the receiver is not a directory, a "Not A Directory" error will be returned.
// If there is an existing node with the same name under the directory, a "Already Exist"
// error will be returned
func (n *node) Add(child *node) error {
	if !n.IsDir() {
		return fmt.Errorf("not a directory")
	}

	_, name := path.Split(child.Path)

	if _, ok := n.Children[name]; ok {
		return fmt.Errorf("node exists")
	}

	n.Children[name] = child

	return nil
}
