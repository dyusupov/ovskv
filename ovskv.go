package ovskv

import (
	"fmt"
	"path"
	"strings"
	"strconv"
	"reflect"

	"github.com/ebay/libovsdb"
)

const (
	OP_INSERT string = "insert"
	OP_UPDATE string = "update"
	OP_DELETE string = "delete"
	OP_SELECT string = "select"
	SEPA string = "/"
	OVSSET_SEPA string = ";"
	OVSKV_TAG string = "ovskv"
	OVSKV_UUID string = "_uuid"
)

type OvsKVRow map[string]interface{}
type OvsKVMap map[string]string
type OvsKVRows []OvsKVMap

type OvsKV interface {
	InsertKV(key, val string)  (string, error)
	InsertKVM(key string, val map[string]string)
	SetKV(key, val string)  (string, error)
	SetKVM(key string, val map[string]string)
	DeleteKV(op, key string) (int, error)
	GetKV(op, key string) (OvsKVRow, error)
	GetKVM(op, key string) (*[]libovsdb.ResultRow, error)
	Save() error
	SaveField(field interface{}) error
	Load() error
	LoadField(data interface{}, prefix string) error
	Disconnect()
}

type OvsKVImpl struct {
	db_name      string
	db_connect   string
	db_namespace string
	ovs          *libovsdb.OvsdbClient
	info         map[string]info
	data         reflect.Value
}

// to keep introspected data
type info struct {
	field   reflect.Value
	version uint32
}

func isTransactError(reply []libovsdb.OperationResult, err error, operations ...[]libovsdb.Operation) error {
	if err != nil {
		return err
	}
	errStr := ""
	if len(reply) < len(operations) {
		errStr = fmt.Sprintf("Number of Replies should be atleast equal to number of Operations\n")
	}
	fail := false
	if len(reply) > 0 {
		for i, o := range reply {
			if o.Error != "" && i < len(operations) {
				errStr += fmt.Sprintf("Transaction Failed due to an error : %v details: %v in: %v\n", o.Error, o.Details, operations[i])
				fail = true
			} else if o.Error != "" {
				errStr += fmt.Sprintf("Transaction Failed due to an error : %v\n", o.Error)
				fail = true
			}
		}
	} else {
		fail = true
	}
	if fail {
		return fmt.Errorf(errStr)
	}
	return nil
}

// XXX: this can be improved with hash function to enable uniform distribution
//      of keys across more then one table of the same type.
func (o *OvsKVImpl) shardTable() string {
	return o.db_namespace + "1"
}

// format OVSDB path Set such so that it can be filtered back in
// ordered non-overlapping ways, like a regular condition request /a/b/c
func pathFmt(key string) (*libovsdb.OvsSet, error) {
	parts := strings.Split(key, SEPA)
	for i, _ := range parts {
		parts[i] = strconv.Itoa(i) + OVSSET_SEPA + parts[i]
	}
        return libovsdb.NewOvsSet(parts)
}

// return key from path
func pathKey(path interface {}) string {
	if reflect.ValueOf(path).Kind() == reflect.String {
		return strings.Split(path.(string), OVSSET_SEPA)[1]
	}
	p := path.(libovsdb.OvsSet).GoSet
	key := ""
	for i, v := range p {
		key += strings.Split(v.(string), OVSSET_SEPA)[1]
		if i < len(p)-1 {
			key += SEPA
		}
	}
	return key
}

func (o *OvsKVImpl) InsertKVM(key string, val map[string]string) (string, error) {
	var err error

	kvRow := make(OvsKVRow)
	kvRow["path"], err = pathFmt(key)
	if err != nil {
		return "", fmt.Errorf("path error: %v\n", err)
	}
	kvRow["data"], err = libovsdb.NewOvsMap(val)
	if err != nil {
		return "", fmt.Errorf("data error: %v\n", err)
	}

	insertOp := libovsdb.Operation{
		Op:       OP_INSERT,
		Table:    o.shardTable(),
		Row:      kvRow,
	}
	reply, err := o.ovs.Transact(o.db_name, insertOp)
	err = isTransactError(reply, err, []libovsdb.Operation{insertOp})
	if err != nil {
		return "", err
	}
	return reply[0].UUID.GoUUID, nil
}

func (o *OvsKVImpl) InsertKV(key, val string) (string, error) {
	return o.InsertKVM(key, o.V(val))
}

func (o *OvsKVImpl) SetKVM(key string, val map[string]string) (string, error) {
	var err error

	kvRow := make(OvsKVRow)
	kvRow["path"], err = pathFmt(key)
	if err != nil {
		return "", fmt.Errorf("path error: %v\n", err)
	}
	kvRow["data"], err = libovsdb.NewOvsMap(val)
	if err != nil {
		return "", fmt.Errorf("data error: %v\n", err)
	}

	// update if exists
	pathSet, err := pathFmt(key)
	if err != nil {
		return "", fmt.Errorf("path error: %v\n", err)
	}
        condition := libovsdb.NewCondition("path", "==", pathSet)
	updateOp := libovsdb.Operation{
		Op:       OP_UPDATE,
		Table:    o.shardTable(),
                Where:    []interface{}{condition},
		Row:      kvRow,
	}
	reply, err := o.ovs.Transact(o.db_name, updateOp)
	err = isTransactError(reply, err, []libovsdb.Operation{updateOp})
	if err != nil {
		return "", err
	}
	if reply[0].Count == 1 {
		return reply[0].UUID.GoUUID, nil
	} else if reply[0].Count > 1 {
		return "", fmt.Errorf("Unable update multi-key: %s\n", key)
	}

	// insert new
	insertOp := libovsdb.Operation{
		Op:       OP_INSERT,
		Table:    o.shardTable(),
		Row:      kvRow,
	}
	reply, err = o.ovs.Transact(o.db_name, insertOp)
	err = isTransactError(reply, err, []libovsdb.Operation{insertOp})
	if err != nil {
		return "", err
	}
	return reply[0].UUID.GoUUID, nil
}

func (o *OvsKVImpl) SetKV(key, val string) (string, error) {
	return o.SetKVM(key, o.V(val))
}

func (o *OvsKVImpl) DeleteKV(op, key string) (int, error) {
	pathSet, err := pathFmt(key)
	if err != nil {
		return 0, fmt.Errorf("path error: %v\n", err)
	}
        condition := libovsdb.NewCondition("path", op, pathSet)
        deleteOp := libovsdb.Operation{
                Op:    OP_DELETE,
		Table:    o.shardTable(),
                Where: []interface{}{condition},
        }
        reply, err := o.ovs.Transact(o.db_name, deleteOp)
	err = isTransactError(reply, err, []libovsdb.Operation{deleteOp})
	if err != nil {
		return 0, err
	}
	return reply[0].Count, nil
}

func (o *OvsKVImpl) GetKV(op, key string) (OvsKVRows, error) {
	var condition []interface{}
	pathSet, err := pathFmt(key)
	if err != nil {
		return nil, err
	}
	condition = libovsdb.NewCondition("path", op, pathSet)
        selectOp := libovsdb.Operation{
                Op:      OP_SELECT,
		Table:    o.shardTable(),
                Where:   []interface{}{condition},
                Columns: []string{"_uuid","path","data"},
        }
        reply, err := o.ovs.Transact(o.db_name, selectOp)
	err = isTransactError(reply, err, []libovsdb.Operation{selectOp})
	if err != nil {
		return nil, err
	}
	res := make(OvsKVRows, len(reply[0].Rows))
	for i, r := range reply[0].Rows {
		row := make(OvsKVMap)
		row["key"] = pathKey(r["path"])
		row["value"] = fmt.Sprintf("%v", r["data"].(libovsdb.OvsMap).GoMap["v"])
		row["uuid"] = r["_uuid"].(libovsdb.UUID).GoUUID
		res[i] = row
        }
	return res, nil
}

func (o *OvsKVImpl) GetKVM(op, key string) (*[]libovsdb.ResultRow, error) {
	pathSet, err := pathFmt(key)
        condition := libovsdb.NewCondition("path", op, pathSet)
        selectOp := libovsdb.Operation{
                Op:      OP_SELECT,
		Table:    o.shardTable(),
                Where:   []interface{}{condition},
                Columns: []string{"_uuid","path","data"},
        }
        reply, err := o.ovs.Transact(o.db_name, selectOp)
	err = isTransactError(reply, err, []libovsdb.Operation{selectOp})
	if err != nil {
		return nil, err
	}
	return &reply[0].Rows, nil
}

// checkDir will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, this function will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
func checkDir(parent *node, dirName string) (*node, error) {
	node, ok := parent.Children[dirName]

	if ok {
		if node.IsDir() {
			return node, nil
		}

		return nil, fmt.Errorf("not a directory")
	}

	n := newDir(path.Join(parent.Path, dirName), 0, parent)

	parent.Children[dirName] = n

	return n, nil
}

// walk walks all the nodePath and apply the walkFunc on each directory
func walk(root *node, nodePath string, walkFunc func(prev *node, component string) (*node, error)) (*node, error) {
	components := strings.Split(nodePath, "/")

	curr := root
	var err error

	for i := 1; i < len(components); i++ {
		if len(components[i]) == 0 { // ignore empty string
			return curr, nil
		}

		curr, err = walkFunc(curr, components[i])
		if err != nil {
			return nil, err
		}
	}

	return curr, nil
}

func (o *OvsKVImpl) GetKVNodes(op, key string) (*node, error) {
	rows, err := o.GetKVM(op, key)
	if err != nil {
		return nil, err
	}

	root := newDir("/", 0, nil)
	for i, r := range *rows {
		nodePath := pathKey(r["path"])

		dirName, nodeName := path.Split(nodePath)

		// walk through the nodePath, create dirs and get the last directory node
		d, err := walk(root, dirName, checkDir)
		if err != nil {
			return nil, err
		}

		n, _ := d.GetChild(nodeName)
		if n != nil {
			continue
		}

		n = newKV(nodePath, &(*rows)[i], 0, d)

		// we are sure d is a directory and does not have the children with name n.Name
		if err := d.Add(n); err != nil {
			return nil, err
		}
	}
	return root, nil
}

func (o *OvsKVImpl) Disconnect() {
        o.ovs.Disconnect()
}

func (o *OvsKVImpl) V(v string) map[string]string {
	return map[string]string{"v": v}
}

// Save stores a structure in ovskv.
// Only attributes with the tag 'ovskv' are going to be saved.
func (o *OvsKVImpl) Save() error {
	return o.saveField(o.data, "")
}

// SaveField saves a specific field from the configuration structure.
// Works in the same way of Save, but it can be used to save specific parts of the configuration,
// avoiding excessive requests to ovsdb cluster
func (o *OvsKVImpl) SaveField(field interface{}) error {
	path, _, err := o.getInfo(field)
	if err != nil {
		return err
	}

	return o.saveField(reflect.ValueOf(field), path)
}

func (o *OvsKVImpl) saveField(field reflect.Value, prefix string) error {
	if field.Kind() == reflect.Ptr {
		field = field.Elem()
	}

	switch field.Kind() {

	case reflect.Struct:
		for i := 0; i < field.NumField(); i++ {
			subfield := field.Field(i)
			subfieldType := field.Type().Field(i)

			path := normalizeTag(subfieldType.Tag.Get(OVSKV_TAG))
			if len(path) == 0 {
				continue
			}
			path = prefix + "/" + path

			if err := o.saveField(subfield, path); err != nil {
				return err
			}
		}

	case reflect.Map:
		for _, key := range field.MapKeys() {
			value := field.MapIndex(key)

			if value.Kind() == reflect.Struct {
				path := prefix + "/" + key.String()
				if err := o.saveField(value, path); err != nil {
					return err
				}
			} else {
				m := make(map[string]string)
				for _, key := range field.MapKeys() {
					value := field.MapIndex(key)
					m[key.String()] = fmt.Sprintf("%v", value)
				}
				if _, err := o.SetKVM(prefix, m); err != nil {
					return err
				}
				break
			}
		}

	case reflect.Slice:
		for i := 0; i < field.Len(); i++ {
			item := field.Index(i)

			if item.Kind() == reflect.Struct {
				path := fmt.Sprintf("%s/%d", prefix, i)

				if err := o.saveField(item, path); err != nil {
					return err
				}
			} else {
				m := make(map[string]string)
				for i := 0; i < field.Len(); i++ {
					item := field.Index(i)
					m[strconv.Itoa(i)] = fmt.Sprintf("%v", item)
				}
				if _, err := o.SetKVM(prefix, m); err != nil {
					return err
				}
				break
			}
		}

	case reflect.String:
		value := field.Interface().(string)
		if _, err := o.SetKV(prefix, value); err != nil {
			return err
		}

	case reflect.Int:
		value := field.Interface().(int)
		if _, err := o.SetKV(prefix, strconv.FormatInt(int64(value), 10)); err != nil {
			return err
		}

	case reflect.Int64:
		value := field.Interface().(int64)
		if _, err := o.SetKV(prefix, strconv.FormatInt(value, 10)); err != nil {
			return err
		}

	case reflect.Bool:
		value := field.Interface().(bool)

		var valueStr string
		if value {
			valueStr = "true"
		} else {
			valueStr = "false"
		}

		if _, err := o.SetKV(prefix, valueStr); err != nil {
			return err
		}
	}

	o.info[prefix] = info{
		field: field,
	}

	return nil
}

func (o *OvsKVImpl) preload(field reflect.Value, prefix string) {
	field = field.Elem()

	switch field.Kind() {
	case reflect.Struct:
		for i := 0; i < field.NumField(); i++ {
			subfield := field.Field(i)
			subfieldType := field.Type().Field(i)

			path := normalizeTag(subfieldType.Tag.Get(OVSKV_TAG))
			if len(path) == 0 {
				continue
			}
			path = prefix + "/" + path

			o.preload(subfield.Addr(), path)
		}
	case reflect.Slice:
		for i := 0; i < field.Len(); i++ {
			subfield := field.Index(i)
			path := prefix + "/" + strconv.Itoa(i)
			o.preload(subfield.Addr(), path)
		}
	}

	if len(prefix) == 0 {
		prefix = "/"
	}

	o.info[prefix] = info{
		field: field,
	}
}

func (o *OvsKVImpl) getInfo(field interface{}) (path string, info info, err error) {
	fieldValue := reflect.ValueOf(field)
	if fieldValue.Kind() == reflect.Ptr {
		fieldValue = fieldValue.Elem()
	} else if !fieldValue.CanAddr() {
		err = fmt.Errorf("Error: field not address\n")
		return
	}

	found := false
	for path, info = range o.info {
		// Match the pointer, type and name to avoid problems for struct and first field that have the
		// same memory address
		if info.field.Addr().Pointer() == fieldValue.Addr().Pointer() &&
			info.field.Type().Name() == fieldValue.Type().Name() &&
			info.field.Kind() == fieldValue.Kind() {

			found = true
			break
		}
	}

	if !found {
		err = fmt.Errorf("Error: field not mapped\n")
	}

	return
}

// Load retrieves the data from the ovsdb into the given structure.
// Only attributes with the tag 'ovskv' will be filled.
func (o *OvsKVImpl) Load() error {
	return o.load(o.data, "")
}

// LoadField retrieves the specific data from the ovsdb into the given structure.
// Only attributes with the tag 'ovskv' will be filled.
func (o *OvsKVImpl) LoadField(data interface{}, prefix string) error {
	if data != nil {
		dataValue := reflect.ValueOf(data)

		if !dataValue.IsZero() && (dataValue.Kind() != reflect.Ptr || dataValue.Elem().Kind() != reflect.Struct) {
			return fmt.Errorf("Error: invalid data kind: %+v\n", dataValue.Kind())
		}
		o.preload(dataValue, prefix)
		return o.load(dataValue, prefix)
	}
	return o.load(o.data, prefix)
}

func traverseFind(node *node, searchPath string) *node {
	if node == nil {
		return nil;
	}
	if node.Key() == searchPath {
		return node
	}

	for _, child := range node.Children {
		found := traverseFind(child, searchPath)
		if found != nil {
			return found
		}
	}

	return nil
}

func (o *OvsKVImpl) load(data reflect.Value, prefix string) error {
	if data.Kind() != reflect.Ptr {
		return fmt.Errorf("Error: invalid data, expecting ptr\n")
	}

	// load all nodes as one op
	nodes, err := o.GetKVNodes("includes", prefix)
	if err != nil {
		return err
	}

	data = data.Elem()
	for i := 0; i < data.NumField(); i++ {
		field := data.Field(i)
		fieldType := data.Type().Field(i)

		fieldName := normalizeTag(fieldType.Tag.Get(OVSKV_TAG))
		if len(fieldName) == 0 {
			continue
		}
		path := prefix + "/" + fieldName

		node := traverseFind(nodes, path)
		if node == nil {
			panic(fmt.Errorf("expected path %s not found", path))
		}

		if err := o.fillField(field, node, path, fieldName); err != nil {
			return err
		}
	}

	return nil
}

func getPathIdx(path string) int {
	parts := strings.Split(path, SEPA)
	idx, _ := strconv.Atoi(parts[len(parts)-1])
	return idx
}

func (o *OvsKVImpl) fillField(field reflect.Value, node *node, prefix, fieldName string) error {
	switch field.Kind() {
	case reflect.Struct:
		for i := 0; i < field.NumField(); i++ {
			subfield := field.Field(i)
			subfieldType := field.Type().Field(i)

			fieldName := normalizeTag(subfieldType.Tag.Get(OVSKV_TAG))
			if len(fieldName) == 0 {
				continue
			}
			path := prefix + "/" + fieldName

			for _, child := range node.Children {
				if path == child.Key() {
					if err := o.fillField(subfield, child, path, fieldName); err != nil {
						return err
					}
					break
				}
			}
		}

	case reflect.Map:
		field.Set(reflect.MakeMap(field.Type()))

		switch field.Type().Elem().Kind() {
		case reflect.Struct:
			for _, node := range node.Children {
				newStruct := reflect.New(field.Type().Elem()).Elem()
				if err := o.fillField(newStruct, node, node.Key(), fieldName); err != nil {
					return err
				}

				pathParts := strings.Split(node.Key(), "/")

				field.SetMapIndex(
					reflect.ValueOf(pathParts[len(pathParts)-1]),
					newStruct,
				)
			}

		case reflect.String:
			m := node.Map()
			for k, v := range m {
				field.SetMapIndex(
					reflect.ValueOf(k.(string)),
					reflect.ValueOf(v.(string)),
				)
			}

		case reflect.Int:
			m := node.Map()
			for k, v := range m {
				val, _ := strconv.Atoi(v.(string))
				field.SetMapIndex(
					reflect.ValueOf(k.(string)),
					reflect.ValueOf(val),
				)
			}

		case reflect.Int64:
			m := node.Map()
			for k, v := range m {
				val, err := strconv.ParseInt(v.(string), 10, 64)
				if err != nil {
					return err
				}
				field.SetMapIndex(
					reflect.ValueOf(k.(string)),
					reflect.ValueOf(val),
				)
			}

		case reflect.Bool:
			m := node.Map()
			for k, v := range m {
				var val bool
				if v.(string) == "true" {
					val = true
				} else if v.(string) == "false" {
					val = false
				}
				field.SetMapIndex(
					reflect.ValueOf(k.(string)),
					reflect.ValueOf(val),
				)
			}
		}

	case reflect.Slice:
		switch field.Type().Elem().Kind() {

		case reflect.Struct:
			childrenLen := len(node.Children)
			newSlice := reflect.MakeSlice(field.Type(), childrenLen, childrenLen)
			field.Set(newSlice)

			for _, item := range node.Children {
				// children nodes out of order, so, we need to extract
				// order from the currently processing path
				idx := getPathIdx(item.Path)
				newStruct := reflect.New(field.Type().Elem()).Elem()

			SubitemLoop:
				for _, subitem := range item.Children {
					for j := 0; j < newStruct.NumField(); j++ {
						subfield := newStruct.Field(j)
						subfieldType := newStruct.Type().Field(j)

						fieldName := normalizeTag(subfieldType.Tag.Get(OVSKV_TAG))
						if len(fieldName) == 0 {
							continue
						}
						path := fmt.Sprintf("%s/%d/%s", prefix, idx, fieldName)

						if path == subitem.Key() {
							if err := o.fillField(subfield, subitem, path, fieldName); err != nil {
								return err
							}
							continue SubitemLoop
						}
					}
				}
				newSlice.Index(idx).Set(newStruct)
			}


		case reflect.String:
			m := node.Map()
			newSlice := reflect.MakeSlice(field.Type(), len(m), len(m))
			field.Set(newSlice)
			for k, v := range m {
				idx, _ := strconv.Atoi(k.(string))
				newSlice.Index(idx).Set(reflect.ValueOf(v.(string)))
			}

		case reflect.Int:
			m := node.Map()
			newSlice := reflect.MakeSlice(field.Type(), len(m), len(m))
			field.Set(newSlice)
			for k, v := range m {
				idx, _ := strconv.Atoi(k.(string))
				value, err := strconv.Atoi(v.(string))
				if err != nil {
					return err
				}
				newSlice.Index(idx).Set(reflect.ValueOf(value))
			}

		case reflect.Int64:
			m := node.Map()
			newSlice := reflect.MakeSlice(field.Type(), len(m), len(m))
			field.Set(newSlice)
			for k, v := range m {
				idx, _ := strconv.Atoi(k.(string))
				value, err := strconv.ParseInt(v.(string), 10, 64)
				if err != nil {
					return err
				}
				newSlice.Index(idx).Set(reflect.ValueOf(value))
			}

		case reflect.Bool:
			m := node.Map()
			newSlice := reflect.MakeSlice(field.Type(), len(m), len(m))
			field.Set(newSlice)
			for k, v := range m {
				idx, _ := strconv.Atoi(k.(string))
				if v.(string) == "true" {
					newSlice.Index(idx).Set(reflect.ValueOf(true))
				} else if v.(string) == "false" {
					newSlice.Index(idx).Set(reflect.ValueOf(false))
				}
			}
		}

	case reflect.String:
		if fieldName == OVSKV_UUID {
			field.SetString(node.UUID())
		} else {
			field.SetString(node.Value())
		}

	case reflect.Int, reflect.Int64:
		value, err := strconv.ParseInt(node.Value(), 10, 64)
		if err != nil {
			return err
		}

		field.SetInt(value)

	case reflect.Bool:
		if node.Value() == "true" {
			field.SetBool(true)
		} else if node.Value() == "false" {
			field.SetBool(false)
		}

	default:
		panic(fmt.Errorf("not supported field kind: %v", field.Kind()))
	}

	o.info[node.Path] = info{
		field:   field,
	}

	return nil
}

func normalizeTag(tag string) string {
	for strings.HasPrefix(tag, "/") {
		tag = strings.TrimPrefix(tag, "/")
	}

	for strings.HasSuffix(tag, "/") {
		tag = strings.TrimSuffix(tag, "/")
	}

	return strings.Replace(tag, "/", "-", -1)
}

func Init(db_name, db_connect, db_namespace string, data interface{}) (*OvsKVImpl, error) {
	var imp = &OvsKVImpl{
		db_name:      db_name,
		db_connect:   db_connect,
		db_namespace: db_namespace,
		info:         make(map[string]info),
	}

	if data != nil {
		dataValue := reflect.ValueOf(data)

		if !dataValue.IsZero() && (dataValue.Kind() != reflect.Ptr || dataValue.Elem().Kind() != reflect.Struct) {
			return nil, fmt.Errorf("Error: invalid data kind: %+v\n", dataValue.Kind())
		}
		imp.data = dataValue
		imp.preload(imp.data, "")
	}

	o, err := libovsdb.Connect(db_connect, nil)
	imp.ovs = o
	return imp, err
}
