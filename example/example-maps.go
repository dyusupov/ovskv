package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"gopkg.in/yaml.v2"

	".."
)

type AddressSet struct {
	UUID        string   `ovskv:"_uuid"`
	Name        string   `yaml:"name" json:"name" ovskv:"name"`
	Description string   `yaml:"description" json:"description" ovskv:"description"`
	PrefixIPv4  []string `yaml:"prefixIPv4" json:"prefixIPv4" ovskv:"prefixIPv4"`
}

type OverlayIPv4Subnet struct {
	UUID                string `ovskv:"_uuid"`
	Name                string `yaml:"name" json:"name" ovskv:"name"`
	Description         string `yaml:"description" json:"description" ovskv:"description"`
	SubnetIPv4          string `yaml:"subnetIPv4" json:"subnetIPv4" ovskv:"subnetIPv4"`
	GatewayIPv4         string `yaml:"gatewayIPv4" json:"gatewayIPv4" ovskv:"gatewayIPv4"`
	GatewayMAC          string `yaml:"gatewayMAC" json:"gatewayMac" ovskv:"gatewayMac"`
	UnderlayIPv4        string `yaml:"underlayIPv4" json:"underlayIPv4" ovskv:"underlayIPv4"`
	UnderlayMAC         string `yaml:"underlayMAC" json:"underlayMac" ovskv:"underlayMac"`
	UnderlayGatewayIPv4 string `yaml:"underlayGatewayIPv4" json:"underlayGatewayIPv4" ovskv:"underlayGatewayIPv4"`
}

type NetInterface struct {
	UUID       string `ovskv:"_uuid"`
	Name       string `yaml:"name" json:"name" ovskv:"name"`
	Disabled   bool   `yaml:"disabled" json:"disabled" ovskv:"disabled"`
	MAC        string `yaml:"mac" json:"mac" ovskv:"mac"`
	VID        int    `yaml:"vlan" json:"vlan" ovskv:"vlan"`
	PFNum      int    `yaml:"pfnum" json:"pfnum" ovskv:"pfnum"`
	VFNum      int    `yaml:"vfnum" json:"vfnum" ovskv:"vfnum"`
	SubnetName string `yaml:"subnet" json:"subnet" ovskv:"subnet"`
}

type Chassis struct {
	UUID          string                  `ovskv:"_uuid"`
	HostName      string                  `yaml:"hostname" json:"hostname" ovskv:"hostname"`
	GWPrio        int                     `yaml:"gwprio" json:"gwprio" ovskv:"gwprio"`
	NetInterfaces map[string]NetInterface `yaml:"interfaces" json:"interfaces" ovskv:"interfaces"`
}

type Service struct {
	UUID         string `ovskv:"_uuid"`
	Name         string `yaml:"name" json:"name" ovskv:"name"`
	Description  string `yaml:"description" json:"description" ovskv:"description"`
	Disabled     bool   `yaml:"disabled" json:"disabled" ovskv:"disabled"`
	LogAccess    bool   `yaml:"logAccess" json:"logAccess" ovskv:"logAccess"`
	LogicalEPv4  string `yaml:"logicalEPv4" json:"logicalEPv4" ovskv:"logicalEPv4"`
	PhysicalEPv4 string `yaml:"physicalEPv4" json:"physicalEPv4" ovskv:"physicalEPv4"`
	LogicalMAC   string `yaml:"logicalMAC" json:"logicalMac" ovskv:"logicalMac"`
	NodeMAC      string `yaml:"nodeMAC" json:"nodeMAC" ovskv:"nodeMAC"`
}

type ACL struct {
	UUID        string `ovskv:"_uuid"`
	Name        string `yaml:"name" json:"name" ovskv:"name"`
	Description string `yaml:"description" json:"description" ovskv:"description"`
	Disabled    bool   `yaml:"disabled" json:"disabled" ovskv:"disabled"`
	LogAccess   bool   `yaml:"logAccess" json:"logAccess" ovskv:"logAccess"`
	Priority    int    `yaml:"priority" json:"priority" ovskv:"priority"`
	Type        string `yaml:"type" json:"type" ovskv:"type"`
	SubnetName  string `yaml:"subnetName" json:"subnetName" ovskv:"subnetName"`
	Direction   string `yaml:"direction" json:"direction" ovskv:"direction"`
	Match       string `yaml:"match" json:"match" ovskv:"match"`
	Action      string `yaml:"action" json:"action" ovskv:"action"`
}

type Tenant struct {
	UUID               string                       `ovskv:"_uuid"`
	Name               string                       `yaml:"name" json:"name" ovskv:"name"`
	OverlayIPv4Subnets map[string]OverlayIPv4Subnet `yaml:"overlayIPv4Subnets" json:"overlayIPv4Subnets" ovskv:"overlayIPv4Subnets"`
	Chassis            map[string]Chassis           `yaml:"chassis" json:"chassis" ovskv:"chassis"`
	Services           map[string]Service           `yaml:"services" json:"services" ovskv:"services"`
	ACLs               map[string]ACL               `yaml:"acls" json:"acls" ovskv:"acls"`
}

type NetworkLayout struct {
	AddressSets map[string]AddressSet `yaml:"addressSets" json:"addressSets" ovskv:"addressSets"`
	Tenants     map[string]Tenant     `yaml:"tenants" json:"tenants" ovskv:"tenants"`
}

func DumpZone(zn NetworkLayout, format string) (string, error) {
	var s string

	switch format {
	case "json":

		j, err := json.MarshalIndent(zn, "", " ")
		if err != nil {
			return "", err
		}
		s = string(j)
	case "yaml":
		y, err := yaml.Marshal(zn)
		if err != nil {
			return "", err
		}
		s = string(y)
	default:
		return "", errors.New(fmt.Sprintf("Unknown format: %s", format))
	}

	return s, nil
}

func main() {
	a := NetworkLayout{
		AddressSets: map[string]AddressSet{
			"as1": {
			},
		},
		Tenants: map[string]Tenant{
			"tenant1": {
				OverlayIPv4Subnets: map[string]OverlayIPv4Subnet{
				},
				Chassis: map[string]Chassis{
					"chassis1": {
						NetInterfaces: map[string]NetInterface{
						},
					},
				},
				Services: map[string]Service{
				},
				ACLs: map[string]ACL{
					"acl1": ACL{
					},
				},
			},
		},
	}

	ovs, err := ovskv.Init("TestKV", "tcp:127.0.0.1:6641", "Zone_", &a)
	if err != nil {
		fmt.Printf("Init error: %v\n", err)
		os.Exit(1)
	}

	if len(os.Args) > 1 && os.Args[1] == "dump" {
		err = ovs.Load()
		if err != nil {
			fmt.Printf("Save error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Loaded from OVSKV\n")
		out, err := DumpZone(a, "yaml")
		if err != nil {
			fmt.Printf("Dump error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(out)
	} else {
		err = ovs.Save()
		if err != nil {
			fmt.Printf("Save error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Saved to OVSKV\n")
	}

        ovs.Disconnect()
}
