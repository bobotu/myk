package art

import "unsafe"

func (n *node) firstChild() *node {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		return n4.children[0]
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		return n16.children[0]
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		for i := 0; i < 256; i++ {
			pos := n48.index[i]
			if pos == 0 {
				continue
			}
			return n48.children[pos-1]
		}
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		for i := 0; i < 256; i++ {
			if c := n256.children[i]; c != nil {
				return c
			}
		}
	}
	panic("unreachable code.")
}

func (n *node) lastChild() *node {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		return n4.children[n4.numChildren-1]
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		return n16.children[n16.numChildren-1]
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		for i := 255; i >= 0; i-- {
			pos := n48.index[i]
			if pos == 0 {
				continue
			}
			if c := n48.children[pos-1]; c != nil {
				return c
			}
		}
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		for i := 255; i >= 0; i-- {
			if c := n256.children[i]; c != nil {
				return c
			}
		}
	}
	panic("unreachable code.")
}
