package stateapis;

public class ListStateAccessor<T> extends BaseStateAccessor<DoublyLinkedList<T>> {
    private KVProvider kvProvider;

    public ListStateAccessor(String descriptorName, KVProvider kvProvider){
        super(descriptorName,kvProvider);
    }
    
    @Override
    public DoublyLinkedList<T> value() {
        return (DoublyLinkedList<T>) kvProvider.get(descriptorName);
    }

    @Override
    public void update(DoublyLinkedList<T> value) {
        kvProvider.put(descriptorName,value);
    }

    @Override
    public void clear() {
        kvProvider.clear();
    }

    public void addFront(T value) {
        DoublyLinkedList<T> doublyLinkedList = this.value();
        doublyLinkedList.addFront(value);
    }

    public void addBack(T value) {
        DoublyLinkedList<T> doublyLinkedList = this.value();
        doublyLinkedList.addBack(value);
    }

    public void remove(T value) {
        DoublyLinkedList<T> doublyLinkedList = this.value();
        doublyLinkedList.remove(value);
    }
}


class DoublyLinkedList<T> {
    private Node head;
    private Node tail;

    private class Node {
        private T value;
        private Node prev;
        private Node next;

        public Node(T value) {
            this.value = value;
            this.prev = null;
            this.next = null;
        }
    }

    public DoublyLinkedList() {
        this.head = null;
        this.tail = null;
    }

    public void addFront(T value) {
        Node node = new Node(value);

        if (head == null) {
            head = node;
            tail = node;
        } else {
            head.prev = node;
            node.next = head;
            head = node;
        }
    }

    public void addBack(T value) {
        Node node = new Node(value);

        if (head == null) {
            head = node;
            tail = node;
        } else {
            tail.next = node;
            node.prev = tail;
            tail = node;
        }
    }

    public void remove(T value) {
        Node current = head;

        while (current != null) {
            if (current.value == value) {
                if (current == head) {
                    head = current.next;
                }
                if (current == tail) {
                    tail = current.prev;
                }
                if (current.prev != null) {
                    current.prev.next = current.next;
                }
                if (current.next != null) {
                    current.next.prev = current.prev;
                }
                break;
            }
            current = current.next;
        }
    }

    public void printList() {
        Node current = head;

        while (current != null) {
            System.out.print(current.value + " ");
            current = current.next;
        }
        System.out.println();
    }

}
