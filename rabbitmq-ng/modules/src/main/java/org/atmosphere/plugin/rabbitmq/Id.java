package org.atmosphere.plugin.rabbitmq;

class Id {
    final String id;

    Id(final String id) {
        this.id = id;
    }

    String getId() {
        return id;
    }

    @Override
    public boolean equals(final Object o) {
        return this == o
                || !(o == null || getClass() != o.getClass())
                && id.equals(((Id) o).id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
